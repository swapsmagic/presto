/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.hive;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.parquet.column.ParquetProperties;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.facebook.presto.hive.HiveClientConfig.InsertExistingPartitionsBehavior;
import static com.facebook.presto.hive.OrcFileWriterConfig.DEFAULT_COMPRESSION_LEVEL;
import static com.facebook.presto.hive.metastore.MetastoreUtil.METASTORE_HEADERS;
import static com.facebook.presto.hive.metastore.MetastoreUtil.USER_DEFINED_TYPE_ENCODING_ENABLED;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.stringProperty;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class HiveSessionProperties
{
    private static final String IGNORE_TABLE_BUCKETING = "ignore_table_bucketing";
    private static final String MIN_BUCKET_COUNT_TO_NOT_IGNORE_TABLE_BUCKETING = "min_bucket_count_to_not_ignore_table_bucketing";
    private static final String BUCKET_EXECUTION_ENABLED = "bucket_execution_enabled";
    public static final String INSERT_EXISTING_PARTITIONS_BEHAVIOR = "insert_existing_partitions_behavior";
    private static final String ORC_STRING_STATISTICS_LIMIT = "orc_string_statistics_limit";
    private static final String ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE = "orc_optimized_writer_min_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE = "orc_optimized_writer_max_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS = "orc_optimized_writer_max_stripe_rows";
    private static final String ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY = "orc_optimized_writer_max_dictionary_memory";
    private static final String ORC_OPTIMIZED_WRITER_INTEGER_DICTIONARY_ENCODING_ENABLED = "orc_optimized_writer_integer_dictionary_encoding_enabled";
    private static final String ORC_OPTIMIZED_WRITER_STRING_DICTIONARY_ENCODING_ENABLED = "orc_optimized_writer_string_dictionary_encoding_enabled";
    private static final String ORC_OPTIMIZED_WRITER_STRING_DICTIONARY_SORTING_ENABLED = "orc_optimized_writer_string_dictionary_sorting_enabled";
    private static final String ORC_OPTIMIZED_WRITER_FLAT_MAP_WRITER_ENABLED = "orc_optimized_writer_flat_map_writer_enabled";
    private static final String ORC_OPTIMIZED_WRITER_COMPRESSION_LEVEL = "orc_optimized_writer_compression_level";
    private static final String PAGEFILE_WRITER_MAX_STRIPE_SIZE = "pagefile_writer_max_stripe_size";
    public static final String HIVE_STORAGE_FORMAT = "hive_storage_format";
    private static final String COMPRESSION_CODEC = "compression_codec";
    private static final String ORC_COMPRESSION_CODEC = "orc_compression_codec";
    public static final String RESPECT_TABLE_FORMAT = "respect_table_format";
    private static final String CREATE_EMPTY_BUCKET_FILES = "create_empty_bucket_files";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String PARQUET_OPTIMIZED_WRITER_ENABLED = "parquet_optimized_writer_enabled";
    private static final String PARQUET_WRITER_VERSION = "parquet_writer_version";
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";
    private static final String SYMLINK_OPTIMIZED_READER_ENABLED = "symlink_optimized_reader_enabled";
    public static final String RCFILE_OPTIMIZED_WRITER_ENABLED = "rcfile_optimized_writer_enabled";
    private static final String RCFILE_OPTIMIZED_WRITER_VALIDATE = "rcfile_optimized_writer_validate";
    private static final String SORTED_WRITING_ENABLED = "sorted_writing_enabled";
    public static final String SORTED_WRITE_TO_TEMP_PATH_ENABLED = "sorted_write_to_temp_path_enabled";
    public static final String SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT = "sorted_write_temp_path_subdirectory_count";
    private static final String STATISTICS_ENABLED = "statistics_enabled";
    private static final String PARTITION_STATISTICS_SAMPLE_SIZE = "partition_statistics_sample_size";
    private static final String IGNORE_CORRUPTED_STATISTICS = "ignore_corrupted_statistics";
    public static final String COLLECT_COLUMN_STATISTICS_ON_WRITE = "collect_column_statistics_on_write";
    public static final String PARTITION_STATISTICS_BASED_OPTIMIZATION_ENABLED = "partition_stats_based_optimization_enabled";
    private static final String OPTIMIZE_MISMATCHED_BUCKET_COUNT = "optimize_mismatched_bucket_count";
    private static final String S3_SELECT_PUSHDOWN_ENABLED = "s3_select_pushdown_enabled";
    public static final String ORDER_BASED_EXECUTION_ENABLED = "order_based_execution_enabled";
    public static final String SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE = "shuffle_partitioned_columns_for_table_write";
    public static final String TEMPORARY_STAGING_DIRECTORY_ENABLED = "temporary_staging_directory_enabled";
    private static final String TEMPORARY_STAGING_DIRECTORY_PATH = "temporary_staging_directory_path";
    private static final String TEMPORARY_TABLE_SCHEMA = "temporary_table_schema";
    private static final String TEMPORARY_TABLE_STORAGE_FORMAT = "temporary_table_storage_format";
    private static final String TEMPORARY_TABLE_COMPRESSION_CODEC = "temporary_table_compression_codec";
    private static final String TEMPORARY_TABLE_CREATE_EMPTY_BUCKET_FILES = "temporary_table_create_empty_bucket_files";
    private static final String USE_PAGEFILE_FOR_HIVE_UNSUPPORTED_TYPE = "use_pagefile_for_hive_unsupported_type";
    public static final String PUSHDOWN_FILTER_ENABLED = "pushdown_filter_enabled";
    public static final String PARQUET_PUSHDOWN_FILTER_ENABLED = "parquet_pushdown_filter_enabled";
    public static final String ADAPTIVE_FILTER_REORDERING_ENABLED = "adaptive_filter_reordering_enabled";
    public static final String VIRTUAL_BUCKET_COUNT = "virtual_bucket_count";
    public static final String CTE_VIRTUAL_BUCKET_COUNT = "cte_virtual_bucket_count";
    public static final String MAX_BUCKETS_FOR_GROUPED_EXECUTION = "max_buckets_for_grouped_execution";
    public static final String OFFLINE_DATA_DEBUG_MODE_ENABLED = "offline_data_debug_mode_enabled";
    public static final String FAIL_FAST_ON_INSERT_INTO_IMMUTABLE_PARTITIONS_ENABLED = "fail_fast_on_insert_into_immutable_partitions_enabled";
    public static final String USE_LIST_DIRECTORY_CACHE = "use_list_directory_cache";
    private static final String BUCKET_FUNCTION_TYPE_FOR_EXCHANGE = "bucket_function_type_for_exchange";
    private static final String BUCKET_FUNCTION_TYPE_FOR_CTE_MATERIALIZATON = "bucket_function_type_for_cte_materialization";
    public static final String PARQUET_DEREFERENCE_PUSHDOWN_ENABLED = "parquet_dereference_pushdown_enabled";
    public static final String IGNORE_UNREADABLE_PARTITION = "ignore_unreadable_partition";
    public static final String PARTIAL_AGGREGATION_PUSHDOWN_ENABLED = "partial_aggregation_pushdown_enabled";
    public static final String PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED = "partial_aggregation_pushdown_for_variable_length_datatypes_enabled";
    public static final String FILE_RENAMING_ENABLED = "file_renaming_enabled";
    public static final String PREFER_MANIFESTS_TO_LIST_FILES = "prefer_manifests_to_list_files";
    public static final String MANIFEST_VERIFICATION_ENABLED = "manifest_verification_enabled";
    public static final String NEW_PARTITION_USER_SUPPLIED_PARAMETER = "new_partition_user_supplied_parameter";
    public static final String OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED = "optimized_partition_update_serialization_enabled";
    public static final String PARTITION_LEASE_DURATION = "partition_lease_duration";
    public static final String CACHE_ENABLED = "cache_enabled";
    public static final String ENABLE_LOOSE_MEMORY_BASED_ACCOUNTING = "enable_loose_memory_based_accounting";
    public static final String MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD = "materialized_view_missing_partitions_threshold";
    public static final String VERBOSE_RUNTIME_STATS_ENABLED = "verbose_runtime_stats_enabled";
    private static final String DWRF_WRITER_STRIPE_CACHE_ENABLED = "dwrf_writer_stripe_cache_enabled";
    private static final String DWRF_WRITER_STRIPE_CACHE_SIZE = "dwrf_writer_stripe_cache_size";
    public static final String USE_COLUMN_INDEX_FILTER = "use_column_index_filter";
    public static final String SIZE_BASED_SPLIT_WEIGHTS_ENABLED = "size_based_split_weights_enabled";
    public static final String MINIMUM_ASSIGNED_SPLIT_WEIGHT = "minimum_assigned_split_weight";
    private static final String USE_RECORD_PAGE_SOURCE_FOR_CUSTOM_SPLIT = "use_record_page_source_for_custom_split";
    public static final String MAX_INITIAL_SPLITS = "max_initial_splits";
    public static final String FILE_SPLITTABLE = "file_splittable";
    private static final String HUDI_METADATA_ENABLED = "hudi_metadata_enabled";
    private static final String HUDI_TABLES_USE_MERGED_VIEW = "hudi_tables_use_merged_view";
    private static final String READ_TABLE_CONSTRAINTS = "read_table_constraints";
    public static final String PARALLEL_PARSING_OF_PARTITION_VALUES_ENABLED = "parallel_parsing_of_partition_values_enabled";
    public static final String QUICK_STATS_ENABLED = "quick_stats_enabled";
    public static final String QUICK_STATS_INLINE_BUILD_TIMEOUT = "quick_stats_inline_build_timeout";
    public static final String QUICK_STATS_BACKGROUND_BUILD_TIMEOUT = "quick_stats_background_build_timeout";
    public static final String DYNAMIC_SPLIT_SIZES_ENABLED = "dynamic_split_sizes_enabled";
    public static final String SKIP_EMPTY_FILES = "skip_empty_files";
    public static final String LEGACY_TIMESTAMP_BUCKETING = "legacy_timestamp_bucketing";
    public static final String OPTIMIZE_PARSING_OF_PARTITION_VALUES = "optimize_parsing_of_partition_values";
    public static final String OPTIMIZE_PARSING_OF_PARTITION_VALUES_THRESHOLD = "optimize_parsing_of_partition_values_threshold";

    public static final String NATIVE_STATS_BASED_FILTER_REORDER_DISABLED = "native_stats_based_filter_reorder_disabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HiveSessionProperties(HiveClientConfig hiveClientConfig, OrcFileWriterConfig orcFileWriterConfig, ParquetFileWriterConfig parquetFileWriterConfig, CacheConfig cacheConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        IGNORE_TABLE_BUCKETING,
                        "Ignore table bucketing to enable reading from unbucketed partitions",
                        hiveClientConfig.isIgnoreTableBucketing(),
                        false),
                integerProperty(
                        MIN_BUCKET_COUNT_TO_NOT_IGNORE_TABLE_BUCKETING,
                        "Ignore table bucketing when table bucket count is less than the value specified",
                        hiveClientConfig.getMinBucketCountToNotIgnoreTableBucketing(),
                        true),
                booleanProperty(
                        BUCKET_EXECUTION_ENABLED,
                        "Enable bucket-aware execution: only use a single worker per bucket",
                        hiveClientConfig.isBucketExecutionEnabled(),
                        false),
                new PropertyMetadata<>(
                        INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        "Behavior on insert existing partitions; this session property doesn't control behavior on insert existing unpartitioned table",
                        VARCHAR,
                        InsertExistingPartitionsBehavior.class,
                        hiveClientConfig.getInsertExistingPartitionsBehavior(),
                        false,
                        value -> InsertExistingPartitionsBehavior.valueOf((String) value, hiveClientConfig.isImmutablePartitions()),
                        InsertExistingPartitionsBehavior::toString),
                dataSizeSessionProperty(
                        ORC_STRING_STATISTICS_LIMIT,
                        "ORC: Maximum size of string statistics; drop if exceeding",
                        orcFileWriterConfig.getStringStatisticsLimit(),
                        false),
                dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE,
                        "Experimental: ORC: Min stripe size",
                        orcFileWriterConfig.getStripeMinSize(),
                        false),
                dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE,
                        "Experimental: ORC: Max stripe size",
                        orcFileWriterConfig.getStripeMaxSize(),
                        false),
                integerProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS,
                        "Experimental: ORC: Max stripe row count",
                        orcFileWriterConfig.getStripeMaxRowCount(),
                        false),
                dataSizeSessionProperty(
                        ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY,
                        "Experimental: ORC: Max dictionary memory",
                        orcFileWriterConfig.getDictionaryMaxMemory(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_INTEGER_DICTIONARY_ENCODING_ENABLED,
                        "ORC: Enable integer dictionary encoding",
                        orcFileWriterConfig.isIntegerDictionaryEncodingEnabled(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_STRING_DICTIONARY_ENCODING_ENABLED,
                        "ORC: Enable string dictionary encoding",
                        orcFileWriterConfig.isStringDictionaryEncodingEnabled(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_STRING_DICTIONARY_SORTING_ENABLED,
                        "ORC: Enable string dictionary sorting",
                        orcFileWriterConfig.isStringDictionarySortingEnabled(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_FLAT_MAP_WRITER_ENABLED,
                        "ORC: Enable flat map writer",
                        orcFileWriterConfig.isFlatMapWriterEnabled(),
                        true),
                integerProperty(
                        ORC_OPTIMIZED_WRITER_COMPRESSION_LEVEL,
                        "Experimental: ORC: Compression level, works only for ZSTD and ZLIB compression kinds",
                        orcFileWriterConfig.getCompressionLevel(),
                        false),
                dataSizeSessionProperty(
                        PAGEFILE_WRITER_MAX_STRIPE_SIZE,
                        "PAGEFILE: Max stripe size",
                        hiveClientConfig.getPageFileStripeMaxSize(),
                        false),
                stringProperty(
                        HIVE_STORAGE_FORMAT,
                        "Default storage format for new tables or partitions",
                        hiveClientConfig.getHiveStorageFormat().toString(),
                        false),
                new PropertyMetadata<>(
                        COMPRESSION_CODEC,
                        "The compression codec to use when writing files",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        hiveClientConfig.getCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name),
                new PropertyMetadata<>(
                        ORC_COMPRESSION_CODEC,
                        "The preferred compression codec to use when writing ORC and DWRF files",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        hiveClientConfig.getOrcCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name),
                booleanProperty(
                        RESPECT_TABLE_FORMAT,
                        "Write new partitions using table format rather than default storage format",
                        hiveClientConfig.isRespectTableFormat(),
                        false),
                booleanProperty(
                        CREATE_EMPTY_BUCKET_FILES,
                        "Create empty files for buckets that have no data",
                        hiveClientConfig.isCreateEmptyBucketFiles(),
                        false),
                dataSizeSessionProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetFileWriterConfig.getBlockSize(),
                        false),
                dataSizeSessionProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetFileWriterConfig.getPageSize(),
                        false),
                dataSizeSessionProperty(
                        MAX_SPLIT_SIZE,
                        "Max split size",
                        hiveClientConfig.getMaxSplitSize(),
                        true),
                dataSizeSessionProperty(
                        MAX_INITIAL_SPLIT_SIZE,
                        "Max initial split size",
                        hiveClientConfig.getMaxInitialSplitSize(),
                        true),
                booleanProperty(
                        RCFILE_OPTIMIZED_WRITER_ENABLED,
                        "Experimental: RCFile: Enable optimized writer",
                        hiveClientConfig.isRcfileOptimizedWriterEnabled(),
                        false),
                booleanProperty(
                        RCFILE_OPTIMIZED_WRITER_VALIDATE,
                        "Experimental: RCFile: Validate writer files",
                        hiveClientConfig.isRcfileWriterValidate(),
                        false),
                booleanProperty(
                        SORTED_WRITING_ENABLED,
                        "Enable writing to bucketed sorted tables",
                        hiveClientConfig.isSortedWritingEnabled(),
                        false),
                booleanProperty(
                        SORTED_WRITE_TO_TEMP_PATH_ENABLED,
                        "Enable writing temp files to temp path when writing to bucketed sorted tables",
                        hiveClientConfig.isSortedWriteToTempPathEnabled(),
                        false),
                integerProperty(
                        SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT,
                        "Number of directories per partition for temp files generated by writing sorted table",
                        hiveClientConfig.getSortedWriteTempPathSubdirectoryCount(),
                        false),
                booleanProperty(
                        STATISTICS_ENABLED,
                        "Experimental: Expose table statistics",
                        hiveClientConfig.isTableStatisticsEnabled(),
                        false),
                integerProperty(
                        PARTITION_STATISTICS_SAMPLE_SIZE,
                        "Maximum sample size of the partitions column statistics",
                        hiveClientConfig.getPartitionStatisticsSampleSize(),
                        false),
                booleanProperty(
                        IGNORE_CORRUPTED_STATISTICS,
                        "Experimental: Ignore corrupted statistics rather than failing",
                        hiveClientConfig.isIgnoreCorruptedStatistics(),
                        false),
                booleanProperty(
                        COLLECT_COLUMN_STATISTICS_ON_WRITE,
                        "Experimental: Enables automatic column level statistics collection on write",
                        hiveClientConfig.isCollectColumnStatisticsOnWrite(),
                        false),
                booleanProperty(
                        PARTITION_STATISTICS_BASED_OPTIMIZATION_ENABLED,
                        "Enables partition stats based optimization, including partition pruning and predicate stripping",
                        hiveClientConfig.isPartitionStatisticsBasedOptimizationEnabled(),
                        false),
                booleanProperty(
                        OPTIMIZE_MISMATCHED_BUCKET_COUNT,
                        "Experimental: Enable optimization to avoid shuffle when bucket count is compatible but not the same",
                        hiveClientConfig.isOptimizeMismatchedBucketCount(),
                        false),
                booleanProperty(
                        S3_SELECT_PUSHDOWN_ENABLED,
                        "S3 Select pushdown enabled",
                        hiveClientConfig.isS3SelectPushdownEnabled(),
                        false),
                booleanProperty(
                        ORDER_BASED_EXECUTION_ENABLED,
                        "Enable order-based execution. When it's enabled, hive files become non-splittable and the table ordering properties would be exposed to plan optimizer " +
                                "for features like streaming aggregation and merge join",
                        hiveClientConfig.isOrderBasedExecutionEnabled(),
                        false),
                booleanProperty(
                        TEMPORARY_STAGING_DIRECTORY_ENABLED,
                        "Should use temporary staging directory for write operations",
                        hiveClientConfig.isTemporaryStagingDirectoryEnabled(),
                        false),
                stringProperty(
                        TEMPORARY_STAGING_DIRECTORY_PATH,
                        "Temporary staging directory location",
                        hiveClientConfig.getTemporaryStagingDirectoryPath(),
                        false),
                stringProperty(
                        TEMPORARY_TABLE_SCHEMA,
                        "Schema where to create temporary tables",
                        hiveClientConfig.getTemporaryTableSchema(),
                        false),
                new PropertyMetadata<>(
                        TEMPORARY_TABLE_STORAGE_FORMAT,
                        "Storage format used to store data in temporary tables",
                        VARCHAR,
                        HiveStorageFormat.class,
                        hiveClientConfig.getTemporaryTableStorageFormat(),
                        false,
                        value -> HiveStorageFormat.valueOf(((String) value).toUpperCase()),
                        HiveStorageFormat::name),
                new PropertyMetadata<>(
                        TEMPORARY_TABLE_COMPRESSION_CODEC,
                        "Compression codec used to store data in temporary tables",
                        VARCHAR,
                        HiveCompressionCodec.class,
                        hiveClientConfig.getTemporaryTableCompressionCodec(),
                        false,
                        value -> HiveCompressionCodec.valueOf(((String) value).toUpperCase()),
                        HiveCompressionCodec::name),
                booleanProperty(
                        TEMPORARY_TABLE_CREATE_EMPTY_BUCKET_FILES,
                        "Create empty files when there is no data for temporary table buckets",
                        hiveClientConfig.isCreateEmptyBucketFilesForTemporaryTable(),
                        false),
                booleanProperty(
                        USE_PAGEFILE_FOR_HIVE_UNSUPPORTED_TYPE,
                        "Automatically switch to PAGEFILE format for materialized exchange when encountering unsupported types",
                        hiveClientConfig.getUsePageFileForHiveUnsupportedType(),
                        true),
                booleanProperty(
                        PUSHDOWN_FILTER_ENABLED,
                        "Experimental: enable complex filter pushdown",
                        hiveClientConfig.isPushdownFilterEnabled(),
                        false),
                booleanProperty(
                        PARQUET_PUSHDOWN_FILTER_ENABLED,
                        "Experimental: enable complex filter pushdown for Parquet",
                        hiveClientConfig.isParquetPushdownFilterEnabled(),
                        false),
                booleanProperty(
                        ADAPTIVE_FILTER_REORDERING_ENABLED,
                        "Experimental: enable adaptive filter reordering",
                        hiveClientConfig.isAdaptiveFilterReorderingEnabled(),
                        false),
                integerProperty(
                        VIRTUAL_BUCKET_COUNT,
                        "Number of virtual bucket assigned for unbucketed tables",
                        0,
                        false),
                integerProperty(
                        CTE_VIRTUAL_BUCKET_COUNT,
                        "Number of virtual bucket assigned for bucketed cte materialization temporary tables",
                        hiveClientConfig.getCteVirtualBucketCount(),
                        false),
                integerProperty(
                        MAX_BUCKETS_FOR_GROUPED_EXECUTION,
                        "maximum total buckets to allow using grouped execution",
                        hiveClientConfig.getMaxBucketsForGroupedExecution(),
                        false),
                booleanProperty(
                        OFFLINE_DATA_DEBUG_MODE_ENABLED,
                        "allow reading from tables or partitions that are marked as offline or not readable",
                        false,
                        true),
                booleanProperty(
                        SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE,
                        "Shuffle the data on partitioned columns",
                        false,
                        false),
                booleanProperty(
                        FAIL_FAST_ON_INSERT_INTO_IMMUTABLE_PARTITIONS_ENABLED,
                        "Fail fast when trying to insert into an immutable partition. Increases load on the metastore",
                        hiveClientConfig.isFailFastOnInsertIntoImmutablePartitionsEnabled(),
                        false),
                booleanProperty(
                        USE_LIST_DIRECTORY_CACHE,
                        "Use list directory cache if available when set to true",
                        !hiveClientConfig.getFileStatusCacheTables().isEmpty(),
                        false),
                booleanProperty(
                        PARQUET_OPTIMIZED_WRITER_ENABLED,
                        "Experimental: Enable optimized writer",
                        parquetFileWriterConfig.isParquetOptimizedWriterEnabled(),
                        false),
                new PropertyMetadata<>(
                        PARQUET_WRITER_VERSION,
                        "Parquet: Writer version",
                        VARCHAR,
                        ParquetProperties.WriterVersion.class,
                        parquetFileWriterConfig.getWriterVersion(),
                        false,
                        value -> ParquetProperties.WriterVersion.valueOf(((String) value).toUpperCase()),
                        ParquetProperties.WriterVersion::name),
                booleanProperty(
                        IGNORE_UNREADABLE_PARTITION,
                        "Ignore unreadable partitions and report as warnings instead of failing the query",
                        hiveClientConfig.isIgnoreUnreadablePartition(),
                        false),
                new PropertyMetadata<>(
                        BUCKET_FUNCTION_TYPE_FOR_EXCHANGE,
                        "hash function type for bucketed table exchange",
                        VARCHAR,
                        BucketFunctionType.class,
                        hiveClientConfig.getBucketFunctionTypeForExchange(),
                        false,
                        value -> BucketFunctionType.valueOf((String) value),
                        BucketFunctionType::toString),
                new PropertyMetadata<>(
                        BUCKET_FUNCTION_TYPE_FOR_CTE_MATERIALIZATON,
                        "hash function type for bucketed table for cte materialization",
                        VARCHAR,
                        BucketFunctionType.class,
                        hiveClientConfig.getBucketFunctionTypeForCteMaterialization(),
                        false,
                        value -> BucketFunctionType.valueOf((String) value),
                        BucketFunctionType::toString),
                booleanProperty(
                        PARQUET_DEREFERENCE_PUSHDOWN_ENABLED,
                        "Is dereference pushdown expression pushdown into Parquet reader enabled?",
                        hiveClientConfig.isParquetDereferencePushdownEnabled(),
                        false),
                booleanProperty(
                        PARTIAL_AGGREGATION_PUSHDOWN_ENABLED,
                        "Is partial aggregation pushdown enabled for Hive file formats",
                        hiveClientConfig.isPartialAggregationPushdownEnabled(),
                        false),
                booleanProperty(
                        PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED,
                        "Is partial aggregation pushdown enabled for variable length datatypes",
                        hiveClientConfig.isPartialAggregationPushdownForVariableLengthDatatypesEnabled(),
                        false),
                booleanProperty(
                        FILE_RENAMING_ENABLED,
                        "Enable renaming the files written by writers",
                        hiveClientConfig.isFileRenamingEnabled(),
                        false),
                booleanProperty(
                        PREFER_MANIFESTS_TO_LIST_FILES,
                        "Prefer to fetch the list of file names and sizes from manifest",
                        hiveClientConfig.isPreferManifestsToListFiles(),
                        false),
                booleanProperty(
                        MANIFEST_VERIFICATION_ENABLED,
                        "Enable manifest verification",
                        hiveClientConfig.isManifestVerificationEnabled(),
                        false),
                stringProperty(
                        NEW_PARTITION_USER_SUPPLIED_PARAMETER,
                        "\"user_supplied\" parameter added to all newly created partitions",
                        null,
                        true),
                booleanProperty(
                        OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED,
                        "Serialize PartitionUpdate objects using binary SMILE encoding and compress with the ZSTD compression",
                        hiveClientConfig.isOptimizedPartitionUpdateSerializationEnabled(),
                        true),
                new PropertyMetadata<>(
                        PARTITION_LEASE_DURATION,
                        "Partition lease duration in seconds, 0 means disabled",
                        VARCHAR,
                        Duration.class,
                        hiveClientConfig.getPartitionLeaseDuration(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(
                        CACHE_ENABLED,
                        "Enable cache for hive",
                        cacheConfig.isCachingEnabled(),
                        false),
                booleanProperty(
                        VERBOSE_RUNTIME_STATS_ENABLED,
                        "Enable tracking all runtime stats. Note that this may affect query performance.",
                        hiveClientConfig.isVerboseRuntimeStatsEnabled(),
                        false),
                booleanProperty(
                        ENABLE_LOOSE_MEMORY_BASED_ACCOUNTING,
                        "Enable loose memory accounting to avoid OOMing existing queries",
                        hiveClientConfig.isLooseMemoryAccountingEnabled(),
                        false),
                integerProperty(
                        MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD,
                        "Materialized views with missing partitions more than this threshold falls back to the base tables at read time",
                        hiveClientConfig.getMaterializedViewMissingPartitionsThreshold(),
                        true),
                stringProperty(
                        METASTORE_HEADERS,
                        "The headers that will be sent in the calls to Metastore",
                        null,
                        false),
                booleanProperty(
                        USER_DEFINED_TYPE_ENCODING_ENABLED,
                        "Enable user defined type",
                        hiveClientConfig.isUserDefinedTypeEncodingEnabled(),
                        false),
                booleanProperty(
                        DWRF_WRITER_STRIPE_CACHE_ENABLED,
                        "Write stripe cache for the DWRF files.",
                        orcFileWriterConfig.isDwrfStripeCacheEnabled(),
                        false),
                dataSizeSessionProperty(
                        DWRF_WRITER_STRIPE_CACHE_SIZE,
                        "Maximum size of DWRF stripe cache to be held in memory",
                        orcFileWriterConfig.getDwrfStripeCacheMaxSize(),
                        false),
                booleanProperty(
                        USE_COLUMN_INDEX_FILTER,
                        "should use column index statistics filtering",
                        hiveClientConfig.getReadColumnIndexFilter(),
                        false),
                booleanProperty(
                        SIZE_BASED_SPLIT_WEIGHTS_ENABLED,
                        "Enable estimating split weights based on size in bytes",
                        hiveClientConfig.isSizeBasedSplitWeightsEnabled(),
                        false),
                booleanProperty(
                        DYNAMIC_SPLIT_SIZES_ENABLED,
                        "Enable dynamic sizing of splits based on column statistics",
                        hiveClientConfig.isDynamicSplitSizesEnabled(),
                        false),
                new PropertyMetadata<>(
                        MINIMUM_ASSIGNED_SPLIT_WEIGHT,
                        "Minimum assigned split weight when size based split weighting is enabled",
                        DOUBLE,
                        Double.class,
                        hiveClientConfig.getMinimumAssignedSplitWeight(),
                        false,
                        value -> {
                            double doubleValue = ((Number) value).doubleValue();
                            if (!Double.isFinite(doubleValue) || doubleValue <= 0 || doubleValue > 1) {
                                throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be > 0 and <= 1.0: %s", MINIMUM_ASSIGNED_SPLIT_WEIGHT, value));
                            }
                            return doubleValue;
                        },
                        value -> value),
                booleanProperty(
                        USE_RECORD_PAGE_SOURCE_FOR_CUSTOM_SPLIT,
                        "Use record page source for custom split",
                        hiveClientConfig.isUseRecordPageSourceForCustomSplit(),
                        false),
                integerProperty(
                        MAX_INITIAL_SPLITS,
                        "Hive max initial split count",
                        hiveClientConfig.getMaxInitialSplits(),
                        true),
                booleanProperty(
                        FILE_SPLITTABLE,
                        "If a hive file is splittable when coordinator schedules splits",
                        hiveClientConfig.isFileSplittable(),
                        true),
                booleanProperty(
                        HUDI_METADATA_ENABLED,
                        "For Hudi tables prefer to fetch the list of file names, sizes and other metadata from the internal metadata table rather than storage",
                        hiveClientConfig.isHudiMetadataEnabled(),
                        false),
                stringProperty(
                        HUDI_TABLES_USE_MERGED_VIEW,
                        "For Hudi tables, a comma-separated list in the form of <schema>.<table> which should use merged view to read data",
                        hiveClientConfig.getHudiTablesUseMergedView(),
                        false),
                booleanProperty(
                        PARALLEL_PARSING_OF_PARTITION_VALUES_ENABLED,
                        "Enables parallel parsing of partition values from partition names using thread pool",
                        hiveClientConfig.isParallelParsingOfPartitionValuesEnabled(),
                        false),
                booleanProperty(
                        QUICK_STATS_ENABLED,
                        "Use quick stats to resolve stats",
                        hiveClientConfig.isQuickStatsEnabled(),
                        false),
                booleanProperty(
                        SYMLINK_OPTIMIZED_READER_ENABLED,
                        "Experimental: Enable optimized SymlinkTextInputFormat reader",
                        hiveClientConfig.isSymlinkOptimizedReaderEnabled(),
                        false),
                new PropertyMetadata<>(
                        QUICK_STATS_INLINE_BUILD_TIMEOUT,
                        "Duration that the first query that initiated a quick stats call should wait before failing and returning EMPTY stats. " +
                                "If set to 0, quick stats builds are pushed to the background, and EMPTY stats are returned",
                        VARCHAR,
                        Duration.class,
                        hiveClientConfig.getQuickStatsInlineBuildTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                new PropertyMetadata<>(
                        QUICK_STATS_BACKGROUND_BUILD_TIMEOUT,
                        "If a quick stats build is already in-progress by another query, this property controls the duration the current query should wait " +
                                "for the in-progress build to finish, before failing and returning EMPTY stats. If set to 0, EMTPY stats are returned whenever an " +
                                "in-progress build is observed",
                        VARCHAR,
                        Duration.class,
                        hiveClientConfig.getQuickStatsBackgroundBuildTimeout(),
                        false,
                        value -> Duration.valueOf((String) value),
                        Duration::toString),
                booleanProperty(
                        SKIP_EMPTY_FILES,
                        "If it is required empty files will be skipped",
                        hiveClientConfig.isSkipEmptyFilesEnabled(),
                        false),
                booleanProperty(
                        LEGACY_TIMESTAMP_BUCKETING,
                        "Use legacy timestamp bucketing algorithm (which is not Hive compatible) for table bucketed by timestamp type.",
                        hiveClientConfig.isLegacyTimestampBucketing(),
                        false),
                booleanProperty(
                        OPTIMIZE_PARSING_OF_PARTITION_VALUES,
                        "Optimize partition values parsing when number of candidates are large",
                        hiveClientConfig.isOptimizeParsingOfPartitionValues(),
                        false),
                integerProperty(OPTIMIZE_PARSING_OF_PARTITION_VALUES_THRESHOLD,
                        "When OPTIMIZE_PARSING_OF_PARTITION_VALUES is set to true, enable this optimizations when number of partitions exceed the threshold here",
                        hiveClientConfig.getOptimizeParsingOfPartitionValuesThreshold(),
                        false),
                booleanProperty(
                        NATIVE_STATS_BASED_FILTER_REORDER_DISABLED,
                        "Native Execution only. Disable stats based filter reordering.",
                        false,
                        true));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isBucketExecutionEnabled(ConnectorSession session)
    {
        return session.getProperty(BUCKET_EXECUTION_ENABLED, Boolean.class);
    }

    public static boolean shouldIgnoreTableBucketing(ConnectorSession session)
    {
        return session.getProperty(IGNORE_TABLE_BUCKETING, Boolean.class);
    }

    public static Integer getMinBucketCountToNotIgnoreTableBucketing(ConnectorSession session)
    {
        return session.getProperty(MIN_BUCKET_COUNT_TO_NOT_IGNORE_TABLE_BUCKETING, Integer.class);
    }

    public static int getMaxBucketsForGroupedExecution(ConnectorSession session)
    {
        return session.getProperty(MAX_BUCKETS_FOR_GROUPED_EXECUTION, Integer.class);
    }

    public static InsertExistingPartitionsBehavior getInsertExistingPartitionsBehavior(ConnectorSession session)
    {
        return session.getProperty(INSERT_EXISTING_PARTITIONS_BEHAVIOR, InsertExistingPartitionsBehavior.class);
    }

    public static DataSize getOrcStringStatisticsLimit(ConnectorSession session)
    {
        return session.getProperty(ORC_STRING_STATISTICS_LIMIT, DataSize.class);
    }

    public static DataSize getOrcOptimizedWriterMinStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE, DataSize.class);
    }

    public static DataSize getOrcOptimizedWriterMaxStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE, DataSize.class);
    }

    public static int getOrcOptimizedWriterMaxStripeRows(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS, Integer.class);
    }

    public static DataSize getOrcOptimizedWriterMaxDictionaryMemory(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY, DataSize.class);
    }

    public static boolean isIntegerDictionaryEncodingEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_INTEGER_DICTIONARY_ENCODING_ENABLED, Boolean.class);
    }

    public static boolean isStringDictionaryEncodingEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_STRING_DICTIONARY_ENCODING_ENABLED, Boolean.class);
    }

    public static boolean isStringDictionarySortingEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_STRING_DICTIONARY_SORTING_ENABLED, Boolean.class);
    }

    public static boolean isFlatMapWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_FLAT_MAP_WRITER_ENABLED, Boolean.class);
    }

    public static OptionalInt getCompressionLevel(ConnectorSession session)
    {
        int value = session.getProperty(ORC_OPTIMIZED_WRITER_COMPRESSION_LEVEL, Integer.class);
        if (value != DEFAULT_COMPRESSION_LEVEL) {
            return OptionalInt.of(value);
        }
        return OptionalInt.empty();
    }

    public static DataSize getPageFileStripeMaxSize(ConnectorSession session)
    {
        return session.getProperty(PAGEFILE_WRITER_MAX_STRIPE_SIZE, DataSize.class);
    }

    public static HiveStorageFormat getHiveStorageFormat(ConnectorSession session)
    {
        return HiveStorageFormat.valueOf(session.getProperty(HIVE_STORAGE_FORMAT, String.class).toUpperCase(ENGLISH));
    }

    public static HiveCompressionCodec getCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static HiveCompressionCodec getOrcCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(ORC_COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static boolean isRespectTableFormat(ConnectorSession session)
    {
        return session.getProperty(RESPECT_TABLE_FORMAT, Boolean.class);
    }

    public static boolean isCreateEmptyBucketFiles(ConnectorSession session)
    {
        return session.getProperty(CREATE_EMPTY_BUCKET_FILES, Boolean.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static DataSize getMaxSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLIT_SIZE, DataSize.class);
    }

    public static DataSize getMaxInitialSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_INITIAL_SPLIT_SIZE, DataSize.class);
    }

    public static boolean isRcfileOptimizedWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(RCFILE_OPTIMIZED_WRITER_ENABLED, Boolean.class);
    }

    public static boolean isRcfileOptimizedWriterValidate(ConnectorSession session)
    {
        return session.getProperty(RCFILE_OPTIMIZED_WRITER_VALIDATE, Boolean.class);
    }

    public static boolean isSortedWritingEnabled(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITING_ENABLED, Boolean.class);
    }

    public static boolean isSortedWriteToTempPathEnabled(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITE_TO_TEMP_PATH_ENABLED, Boolean.class);
    }

    public static int getSortedWriteTempPathSubdirectoryCount(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITE_TEMP_PATH_SUBDIRECTORY_COUNT, Integer.class);
    }

    public static boolean isS3SelectPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(S3_SELECT_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isOrderBasedExecutionEnabled(ConnectorSession session)
    {
        return session.getProperty(ORDER_BASED_EXECUTION_ENABLED, Boolean.class);
    }

    public static boolean isStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(STATISTICS_ENABLED, Boolean.class);
    }

    public static int getPartitionStatisticsSampleSize(ConnectorSession session)
    {
        int size = session.getProperty(PARTITION_STATISTICS_SAMPLE_SIZE, Integer.class);
        if (size < 1) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be greater than 0: %s", PARTITION_STATISTICS_SAMPLE_SIZE, size));
        }
        return size;
    }

    public static boolean isIgnoreCorruptedStatistics(ConnectorSession session)
    {
        return session.getProperty(IGNORE_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static boolean isCollectColumnStatisticsOnWrite(ConnectorSession session)
    {
        return session.getProperty(COLLECT_COLUMN_STATISTICS_ON_WRITE, Boolean.class);
    }

    public static boolean isPartitionStatisticsBasedOptimizationEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTITION_STATISTICS_BASED_OPTIMIZATION_ENABLED, Boolean.class);
    }

    @Deprecated
    public static boolean isOptimizedMismatchedBucketCount(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_MISMATCHED_BUCKET_COUNT, Boolean.class);
    }

    public static boolean isTemporaryStagingDirectoryEnabled(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_STAGING_DIRECTORY_ENABLED, Boolean.class);
    }

    public static String getTemporaryStagingDirectoryPath(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_STAGING_DIRECTORY_PATH, String.class);
    }

    public static String getTemporaryTableSchema(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_TABLE_SCHEMA, String.class);
    }

    public static HiveStorageFormat getTemporaryTableStorageFormat(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_TABLE_STORAGE_FORMAT, HiveStorageFormat.class);
    }

    public static HiveCompressionCodec getTemporaryTableCompressionCodec(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_TABLE_COMPRESSION_CODEC, HiveCompressionCodec.class);
    }

    public static boolean shouldCreateEmptyBucketFilesForTemporaryTable(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_TABLE_CREATE_EMPTY_BUCKET_FILES, Boolean.class);
    }

    public static boolean isUsePageFileForHiveUnsupportedType(ConnectorSession session)
    {
        return session.getProperty(USE_PAGEFILE_FOR_HIVE_UNSUPPORTED_TYPE, Boolean.class);
    }

    public static boolean isPushdownFilterEnabled(ConnectorSession session)
    {
        return session.getProperty(PUSHDOWN_FILTER_ENABLED, Boolean.class);
    }

    public static boolean isParquetPushdownFilterEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_PUSHDOWN_FILTER_ENABLED, Boolean.class);
    }

    public static boolean isAdaptiveFilterReorderingEnabled(ConnectorSession session)
    {
        return session.getProperty(ADAPTIVE_FILTER_REORDERING_ENABLED, Boolean.class);
    }

    public static int getVirtualBucketCount(ConnectorSession session)
    {
        int virtualBucketCount = session.getProperty(VIRTUAL_BUCKET_COUNT, Integer.class);
        if (virtualBucketCount < 0) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must not be negative: %s", VIRTUAL_BUCKET_COUNT, virtualBucketCount));
        }
        return virtualBucketCount;
    }

    public static int getCteVirtualBucketCount(ConnectorSession session)
    {
        int virtualBucketCount = session.getProperty(CTE_VIRTUAL_BUCKET_COUNT, Integer.class);
        if (virtualBucketCount < 0) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must not be negative: %s", CTE_VIRTUAL_BUCKET_COUNT, virtualBucketCount));
        }
        return virtualBucketCount;
    }

    public static boolean isOfflineDataDebugModeEnabled(ConnectorSession session)
    {
        return session.getProperty(OFFLINE_DATA_DEBUG_MODE_ENABLED, Boolean.class);
    }

    public static boolean shouldIgnoreUnreadablePartition(ConnectorSession session)
    {
        return session.getProperty(IGNORE_UNREADABLE_PARTITION, Boolean.class);
    }

    public static boolean isShufflePartitionedColumnsForTableWriteEnabled(ConnectorSession session)
    {
        return session.getProperty(SHUFFLE_PARTITIONED_COLUMNS_FOR_TABLE_WRITE, Boolean.class);
    }

    public static PropertyMetadata<DataSize> dataSizeSessionProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                createUnboundedVarcharType(),
                DataSize.class,
                defaultValue,
                hidden,
                value -> DataSize.valueOf((String) value),
                DataSize::toString);
    }

    public static boolean isFailFastOnInsertIntoImmutablePartitionsEnabled(ConnectorSession session)
    {
        return session.getProperty(FAIL_FAST_ON_INSERT_INTO_IMMUTABLE_PARTITIONS_ENABLED, Boolean.class);
    }

    public static boolean isUseListDirectoryCache(ConnectorSession session)
    {
        return session.getProperty(USE_LIST_DIRECTORY_CACHE, Boolean.class);
    }

    public static boolean isParquetOptimizedWriterEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_OPTIMIZED_WRITER_ENABLED, Boolean.class);
    }

    public static ParquetProperties.WriterVersion getParquetWriterVersion(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_VERSION, ParquetProperties.WriterVersion.class);
    }

    public static BucketFunctionType getBucketFunctionTypeForExchange(ConnectorSession session)
    {
        return session.getProperty(BUCKET_FUNCTION_TYPE_FOR_EXCHANGE, BucketFunctionType.class);
    }

    public static BucketFunctionType getBucketFunctionTypeForCteMaterialization(ConnectorSession session)
    {
        return session.getProperty(BUCKET_FUNCTION_TYPE_FOR_CTE_MATERIALIZATON, BucketFunctionType.class);
    }

    public static boolean isParquetDereferencePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARQUET_DEREFERENCE_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isPartialAggregationPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTIAL_AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isPartialAggregationPushdownForVariableLengthDatatypesEnabled(ConnectorSession session)
    {
        return session.getProperty(PARTIAL_AGGREGATION_PUSHDOWN_FOR_VARIABLE_LENGTH_DATATYPES_ENABLED, Boolean.class);
    }

    public static boolean isFileRenamingEnabled(ConnectorSession session)
    {
        return session.getProperty(FILE_RENAMING_ENABLED, Boolean.class);
    }

    public static boolean isPreferManifestsToListFiles(ConnectorSession session)
    {
        return session.getProperty(PREFER_MANIFESTS_TO_LIST_FILES, Boolean.class);
    }

    public static boolean isManifestVerificationEnabled(ConnectorSession session)
    {
        return session.getProperty(MANIFEST_VERIFICATION_ENABLED, Boolean.class);
    }

    public static Optional<String> getNewPartitionUserSuppliedParameter(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(NEW_PARTITION_USER_SUPPLIED_PARAMETER, String.class));
    }

    public static boolean isOptimizedPartitionUpdateSerializationEnabled(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZED_PARTITION_UPDATE_SERIALIZATION_ENABLED, Boolean.class);
    }

    public static Duration getLeaseDuration(ConnectorSession session)
    {
        return session.getProperty(PARTITION_LEASE_DURATION, Duration.class);
    }

    public static boolean isCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(CACHE_ENABLED, Boolean.class);
    }

    public static boolean isExecutionBasedMemoryAccountingEnabled(ConnectorSession session)
    {
        return session.getProperty(ENABLE_LOOSE_MEMORY_BASED_ACCOUNTING, Boolean.class);
    }

    public static int getMaterializedViewMissingPartitionsThreshold(ConnectorSession session)
    {
        return session.getProperty(MATERIALIZED_VIEW_MISSING_PARTITIONS_THRESHOLD, Integer.class);
    }

    public static boolean isVerboseRuntimeStatsEnabled(ConnectorSession session)
    {
        return session.getProperty(VERBOSE_RUNTIME_STATS_ENABLED, Boolean.class);
    }

    public static boolean isDwrfWriterStripeCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(DWRF_WRITER_STRIPE_CACHE_ENABLED, Boolean.class);
    }

    public static DataSize getDwrfWriterStripeCacheMaxSize(ConnectorSession session)
    {
        return session.getProperty(DWRF_WRITER_STRIPE_CACHE_SIZE, DataSize.class);
    }

    public static boolean columnIndexFilterEnabled(ConnectorSession session)
    {
        return session.getProperty(USE_COLUMN_INDEX_FILTER, Boolean.class);
    }

    public static boolean isSizeBasedSplitWeightsEnabled(ConnectorSession session)
    {
        return session.getProperty(SIZE_BASED_SPLIT_WEIGHTS_ENABLED, Boolean.class);
    }

    public static boolean isDynamicSplitSizesEnabled(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_SPLIT_SIZES_ENABLED, Boolean.class);
    }

    public static double getMinimumAssignedSplitWeight(ConnectorSession session)
    {
        return session.getProperty(MINIMUM_ASSIGNED_SPLIT_WEIGHT, Double.class);
    }

    public static boolean isUseRecordPageSourceForCustomSplit(ConnectorSession session)
    {
        return session.getProperty(USE_RECORD_PAGE_SOURCE_FOR_CUSTOM_SPLIT, Boolean.class);
    }

    public static int getHiveMaxInitialSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_INITIAL_SPLITS, Integer.class);
    }

    public static boolean isFileSplittable(ConnectorSession session)
    {
        return session.getProperty(FILE_SPLITTABLE, Boolean.class);
    }

    public static boolean isHudiMetadataEnabled(ConnectorSession session)
    {
        return session.getProperty(HUDI_METADATA_ENABLED, Boolean.class);
    }

    public static String getHudiTablesUseMergedView(ConnectorSession session)
    {
        String hudiTablesUseMergedView = session.getProperty(HUDI_TABLES_USE_MERGED_VIEW, String.class);
        return hudiTablesUseMergedView == null ? "" : hudiTablesUseMergedView;
    }

    public static boolean isReadTableConstraints(ConnectorSession session)
    {
        return session.getProperty(READ_TABLE_CONSTRAINTS, Boolean.class);
    }

    public static boolean isParallelParsingOfPartitionValuesEnabled(ConnectorSession session)
    {
        return session.getProperty(PARALLEL_PARSING_OF_PARTITION_VALUES_ENABLED, Boolean.class);
    }

    public static boolean isQuickStatsEnabled(ConnectorSession session)
    {
        return session.getProperty(QUICK_STATS_ENABLED, Boolean.class);
    }

    public static Duration getQuickStatsInlineBuildTimeout(ConnectorSession session)
    {
        return session.getProperty(QUICK_STATS_INLINE_BUILD_TIMEOUT, Duration.class);
    }

    public static Duration getQuickStatsBackgroundBuildTimeout(ConnectorSession session)
    {
        return session.getProperty(QUICK_STATS_BACKGROUND_BUILD_TIMEOUT, Duration.class);
    }

    public static boolean isSkipEmptyFilesEnabled(ConnectorSession session)
    {
        return session.getProperty(SKIP_EMPTY_FILES, Boolean.class);
    }

    public static boolean isLegacyTimestampBucketing(ConnectorSession session)
    {
        return session.getProperty(LEGACY_TIMESTAMP_BUCKETING, Boolean.class);
    }

    public static boolean isOptimizeParsingOfPartitionValues(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_PARSING_OF_PARTITION_VALUES, Boolean.class);
    }

    public static int getOptimizeParsingOfPartitionValuesThreshold(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_PARSING_OF_PARTITION_VALUES_THRESHOLD, Integer.class);
    }

    public static boolean isSymlinkOptimizedReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(SYMLINK_OPTIMIZED_READER_ENABLED, Boolean.class);
    }
}
