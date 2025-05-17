// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package catalog

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/polarsignals/iceberg-go"
	"github.com/polarsignals/iceberg-go/table"
	"github.com/thanos-io/objstore"
)

const (
	// AWS Glue catalog constants
	glueTypeIceberg      = "ICEBERG"
	databaseTypePropsKey = "database_type"
	tableTypePropsKey    = "table_type"
	descriptionPropsKey  = "Description"

	// Database location.
	locationPropsKey = "Location"

	// Table metadata location pointer.
	metadataLocationPropsKey = "metadata_location"

	// The ID of the Glue Data Catalog where the tables reside. If none is provided, Glue
	// automatically uses the caller's AWS account ID by default.
	CatalogIdKey = "glue.id"

	AccessKeyID     = "glue.access-key-id"
	SecretAccessKey = "glue.secret-access-key"
	SessionToken    = "glue.session-token"
	Region          = "glue.region"
	Endpoint        = "glue.endpoint"
	MaxRetries      = "glue.max-retries"
	RetryMode       = "glue.retry-mode"
)

type glueCatalog struct {
	client    *glue.Client
	catalogID string
	bucket    objstore.Bucket
	props     iceberg.Properties
}

// NewGlue creates a new AWS Glue catalog implementation.
// region: AWS region where the Glue catalog is located
// catalogID: Optional AWS Glue catalog ID. If empty, defaults to the AWS account ID
// bucket: Object storage bucket for storing Iceberg metadata
// props: Additional properties for the Glue catalog
func NewGlue(region, catalogID string, bucket objstore.Bucket, props iceberg.Properties) (Catalog, error) {
	if props == nil {
		props = make(iceberg.Properties)
	}

	// Add region to properties
	props[Region] = region

	// Add catalog ID to properties if provided
	if catalogID != "" {
		props[CatalogIdKey] = catalogID
	}

	// Create AWS config
	opts := make([]func(*config.LoadOptions) error, 0)
	opts = append(opts, config.WithRegion(region))

	// Add credentials if provided
	if accessKey, ok := props[AccessKeyID]; ok {
		secretKey, ok := props[SecretAccessKey]
		if !ok {
			return nil, fmt.Errorf("secret access key is required when access key ID is provided")
		}

		credOpts := []func(*credentials.Options){}
		if token, ok := props[SessionToken]; ok {
			credOpts = append(credOpts, func(o *credentials.Options) {
				o.SessionToken = token
			})
		}

		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, props[SessionToken]),
		))
	}

	// Add endpoint if provided
	if endpoint, ok := props[Endpoint]; ok {
		opts = append(opts, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(_, _ string, _ ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			}),
		))
	}

	// Add max retries if provided
	if maxRetries, ok := props[MaxRetries]; ok {
		maxRetry, err := strconv.Atoi(maxRetries)
		if err != nil {
			return nil, fmt.Errorf("invalid max retries value %q: %w", maxRetries, err)
		}
		opts = append(opts, config.WithRetryMaxAttempts(maxRetry))
	}

	// Add retry mode if provided
	if retryMode, ok := props[RetryMode]; ok {
		opts = append(opts, config.WithRetryMode(aws.RetryMode(retryMode)))
	}

	// Create AWS config
	cfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS config: %w", err)
	}

	// Create Glue client
	client := glue.NewFromConfig(cfg)

	return &glueCatalog{
		client:    client,
		catalogID: catalogID,
		bucket:    bucket,
		props:     props,
	}, nil
}

func (g *glueCatalog) CatalogType() CatalogType {
	return Glue
}

func (g *glueCatalog) ListTables(ctx context.Context, namespace table.Identifier) ([]table.Identifier, error) {
	if len(namespace) == 0 {
		return nil, fmt.Errorf("namespace cannot be empty")
	}

	database := strings.Join(namespace, namespaceSeparator)
	input := &glue.GetTablesInput{
		DatabaseName: aws.String(database),
	}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	tables := []table.Identifier{}
	paginator := glue.NewGetTablesPaginator(g.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			var notFoundErr *types.EntityNotFoundException
			if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") || err != nil {
				return nil, fmt.Errorf("%w: %s", ErrNoSuchNamespace, err.Error())
			}

			return nil, fmt.Errorf("failed to list tables: %w", err)
		}

		for _, tbl := range output.TableList {
			if isIcebergTable(tbl) {
				tables = append(tables, table.Identifier{*tbl.Name})
			}
		}
	}

	return tables, nil
}

func (g *glueCatalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (table.Table, error) {
	if len(identifier) < 2 {
		return nil, fmt.Errorf("identifier must have at least 2 parts: database and table name")
	}

	database := identifier[0]
	tableName := identifier[1]

	input := &glue.GetTableInput{
		DatabaseName: aws.String(database),
		Name:         aws.String(tableName),
	}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	output, err := g.client.GetTable(ctx, input)
	if err != nil {
		var notFoundErr *types.EntityNotFoundException
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") || err != nil {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchTable, err.Error())
		}
		return nil, fmt.Errorf("failed to get table: %w", err)
	}

	if !isIcebergTable(*output.Table) {
		return nil, fmt.Errorf("table %q is not an Iceberg table", tableName)
	}

	// Get metadata location from table properties
	metadataLocation, ok := output.Table.Parameters[metadataLocationPropsKey]
	if !ok {
		return nil, fmt.Errorf("table does not have metadata location property")
	}

	// Read metadata from the location
	r, err := g.bucket.Get(ctx, *metadataLocation)
	if err != nil {
		return nil, fmt.Errorf("failed to read metadata from %q: %w", *metadataLocation, err)
	}
	defer r.Close()

	// Parse metadata
	metadata, err := table.ParseTableMetadata(r)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	// Create table
	return table.New(identifier, metadata, *metadataLocation, g.bucket), nil
}

func (g *glueCatalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	if len(identifier) < 2 {
		return fmt.Errorf("identifier must have at least 2 parts: database and table name")
	}

	database := identifier[0]
	tableName := identifier[1]

	input := &glue.DeleteTableInput{
		DatabaseName: aws.String(database),
		Name:         aws.String(tableName),
	}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	_, err := g.client.DeleteTable(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete table: %w", err)
	}

	return nil
}

func (g *glueCatalog) RenameTable(ctx context.Context, from, to table.Identifier) (table.Table, error) {
	// Load the source table
	tbl, err := g.LoadTable(ctx, from, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load source table: %w", err)
	}

	// Create the destination table with the same properties
	newTable, err := g.CreateTable(ctx, tbl.Location(), tbl.Schema(), tbl.Properties())
	if err != nil {
		return nil, fmt.Errorf("failed to create destination table: %w", err)
	}

	// Drop the source table
	if err := g.DropTable(ctx, from); err != nil {
		// Try to clean up the destination table if the operation fails
		_ = g.DropTable(ctx, to)
		return nil, fmt.Errorf("failed to drop source table: %w", err)
	}

	return newTable, nil
}

func (g *glueCatalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	input := &glue.GetDatabasesInput{}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	namespaces := []table.Identifier{}
	paginator := glue.NewGetDatabasesPaginator(g.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list databases: %w", err)
		}

		for _, db := range output.DatabaseList {
			namespaces = append(namespaces, table.Identifier{*db.Name})
		}
	}

	return namespaces, nil
}

func (g *glueCatalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	if len(namespace) == 0 {
		return fmt.Errorf("namespace cannot be empty")
	}

	database := strings.Join(namespace, namespaceSeparator)
	input := &glue.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{
			Name:        aws.String(database),
			Description: aws.String(props[descriptionPropsKey]),
			Parameters:  map[string]string{},
		},
	}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	// Set database type to Iceberg
	input.DatabaseInput.Parameters[databaseTypePropsKey] = glueTypeIceberg

	// Set location if provided
	if location, ok := props[locationPropsKey]; ok {
		input.DatabaseInput.LocationUri = aws.String(location)
	}

	// Add all other properties
	for k, v := range props {
		if k != descriptionPropsKey && k != locationPropsKey {
			input.DatabaseInput.Parameters[k] = v
		}
	}

	_, err := g.client.CreateDatabase(ctx, input)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("%w: %s", ErrNamespaceAlreadyExists, err.Error())
		}
		return fmt.Errorf("failed to create database: %w", err)
	}

	return nil
}

func (g *glueCatalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	if len(namespace) == 0 {
		return fmt.Errorf("namespace cannot be empty")
	}

	database := strings.Join(namespace, namespaceSeparator)
	input := &glue.DeleteDatabaseInput{
		Name: aws.String(database),
	}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	_, err := g.client.DeleteDatabase(ctx, input)
	if err != nil {
		var notFoundErr *types.EntityNotFoundException
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") || err != nil {
			return fmt.Errorf("%w: %s", ErrNoSuchNamespace, err.Error())
		}
		return fmt.Errorf("failed to delete database: %w", err)
	}

	return nil
}

func (g *glueCatalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	if len(namespace) == 0 {
		return nil, fmt.Errorf("namespace cannot be empty")
	}

	database := strings.Join(namespace, namespaceSeparator)
	input := &glue.GetDatabaseInput{
		Name: aws.String(database),
	}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	output, err := g.client.GetDatabase(ctx, input)
	if err != nil {
		var notFoundErr *types.EntityNotFoundException
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") || err != nil {
			return nil, fmt.Errorf("%w: %s", ErrNoSuchNamespace, err.Error())
		}
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	props := make(iceberg.Properties)

	// Add description if available
	if output.Database.Description != nil {
		props[descriptionPropsKey] = *output.Database.Description
	}

	// Add location if available
	if output.Database.LocationUri != nil {
		props[locationPropsKey] = *output.Database.LocationUri
	}

	// Add all parameters
	for k, v := range output.Database.Parameters {
		props[k] = v
	}

	return props, nil
}

func (g *glueCatalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (PropertiesUpdateSummary, error) {
	// Load current properties
	props, err := g.LoadNamespaceProperties(ctx, namespace)
	if err != nil {
		return PropertiesUpdateSummary{}, fmt.Errorf("failed to load namespace properties: %w", err)
	}

	summary := PropertiesUpdateSummary{
		Removed: []string{},
		Updated: []string{},
		Missing: []string{},
	}

	// Process removals
	for _, key := range removals {
		if _, ok := props[key]; ok {
			delete(props, key)
			summary.Removed = append(summary.Removed, key)
		} else {
			summary.Missing = append(summary.Missing, key)
		}
	}

	// Process updates
	for key, value := range updates {
		props[key] = value
		summary.Updated = append(summary.Updated, key)
	}

	// Update database
	database := strings.Join(namespace, namespaceSeparator)
	input := &glue.UpdateDatabaseInput{
		Name: aws.String(database),
		DatabaseInput: &types.DatabaseInput{
			Name:       aws.String(database),
			Parameters: map[string]string{},
		},
	}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	// Set description if available
	if desc, ok := props[descriptionPropsKey]; ok {
		input.DatabaseInput.Description = aws.String(desc)
	}

	// Set location if available
	if location, ok := props[locationPropsKey]; ok {
		input.DatabaseInput.LocationUri = aws.String(location)
	}

	// Set parameters
	for k, v := range props {
		if k != descriptionPropsKey && k != locationPropsKey {
			input.DatabaseInput.Parameters[k] = v
		}
	}

	_, err = g.client.UpdateDatabase(ctx, input)
	if err != nil {
		return PropertiesUpdateSummary{}, fmt.Errorf("failed to update database: %w", err)
	}

	return summary, nil
}

func (g *glueCatalog) CreateTable(ctx context.Context, location string, schema *iceberg.Schema, props iceberg.Properties, options ...TableOption) (table.Table, error) {
	if len(props) < 2 {
		return nil, fmt.Errorf("properties must include at least database and table name")
	}

	// Extract database and table name
	database, ok := props["database"]
	if !ok {
		return nil, fmt.Errorf("database name is required in properties")
	}

	tableName, ok := props["table"]
	if !ok {
		return nil, fmt.Errorf("table name is required in properties")
	}

	// Apply table options
	opts := &tableOptions{}
	for _, option := range options {
		option(opts)
	}

	// Create metadata file
	metadata := table.NewTableMetadata(
		table.FormatV2,
		location,
		schema,
		opts.partitionSpec,
		nil, // sort orders
		props,
	)

	// Generate a unique ID for the metadata file
	metadataFile := fmt.Sprintf("%s/metadata/v0.metadata.json", location)

	// Write metadata to the bucket
	metadataBytes, err := metadata.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := g.bucket.Upload(ctx, metadataFile, strings.NewReader(string(metadataBytes))); err != nil {
		return nil, fmt.Errorf("failed to upload metadata: %w", err)
	}

	// Create Glue table
	input := &glue.CreateTableInput{
		DatabaseName: aws.String(database),
		TableInput: &types.TableInput{
			Name:        aws.String(tableName),
			Description: aws.String(props[descriptionPropsKey]),
			Parameters:  map[string]string{},
		},
	}

	if g.catalogID != "" {
		input.CatalogId = aws.String(g.catalogID)
	}

	// Set table type to Iceberg
	input.TableInput.Parameters[tableTypePropsKey] = glueTypeIceberg

	// Set metadata location
	input.TableInput.Parameters[metadataLocationPropsKey] = metadataFile

	// Add all other properties
	for k, v := range props {
		if k != descriptionPropsKey && k != "database" && k != "table" {
			input.TableInput.Parameters[k] = v
		}
	}

	// Create the table in Glue
	_, err = g.client.CreateTable(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to create Glue table: %w", err)
	}

	// Return the table
	identifier := table.Identifier{database, tableName}
	return table.New(identifier, metadata, metadataFile, g.bucket), nil
}

// Helper function to check if a table is an Iceberg table
func isIcebergTable(table types.Table) bool {
	if table.Parameters == nil {
		return false
	}

	// Check for table type
	if tableType, ok := table.Parameters[tableTypePropsKey]; ok {
		return strings.EqualFold(tableType, glueTypeIceberg)
	}

	// Check for metadata location
	_, hasMetadataLocation := table.Parameters[metadataLocationPropsKey]
	return hasMetadataLocation
}
