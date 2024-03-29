using System;
using System.Collections.Generic;
using System.Linq;
using S2.BlackSwan.SupplyCollector.Models;
using Xunit;

namespace HiveSupplyCollectorTests
{
    public class HiveSupplyCollectorTests : IClassFixture<LaunchSettingsFixture>
    {
        private readonly LaunchSettingsFixture _fixture;
        private readonly HiveSupplyCollector.HiveSupplyCollector _instance;
        private readonly DataContainer _container;

        public HiveSupplyCollectorTests(LaunchSettingsFixture fixture)
        {
            _fixture = fixture;
            _instance = new HiveSupplyCollector.HiveSupplyCollector();
            _container = new DataContainer()
            {
                ConnectionString = _instance.BuildConnectionString(
                    Environment.GetEnvironmentVariable("HIVE_HOST"),
                    Int32.Parse(Environment.GetEnvironmentVariable("HIVE_PORT")),
                    Environment.GetEnvironmentVariable("HIVE_USER"),
                    Environment.GetEnvironmentVariable("HIVE_PASS"),
                    Environment.GetEnvironmentVariable("HIVE_DB")
                    )
            };
        }

        [Fact]
        public void DataStoreTypesTest()
        {
            var result = _instance.DataStoreTypes();
            Assert.Contains("Hive", result);
        }

        [Fact]
        public void ConnectionTest()
        {
            Assert.True(_instance.TestConnection(_container));
        }

        [Fact]
        public void GetTableNamesTest()
        {
            var (tables, _) = _instance.GetSchema(_container);
            Assert.Equal(3, tables.Count);

            var tableNames = new [] { "test_data_types", "test_field_names", "test_complex_types" };
            foreach (var tableName in tableNames)
            {
                var table = tables.Find(x => x.Name.Equals(tableName));
                Assert.NotNull(table);
            }
        }
        
        [Fact]
        public void DataTypesTest()
        {
            var (_, elements) = _instance.GetSchema(_container);

            var dataTypes = new Dictionary<string, string>() {
                {"id", "int"},
                {"tinyint_field", "tinyint"},
                {"smallint_field", "smallint"},
                {"int_field", "int"},
                {"bigint_field", "bigint"},
                {"bool_field", "boolean"},
                {"float_field", "float"},
                {"double_field", "double"},
                {"decimal_field", "decimal(10,0)"},
                {"string_field", "string"},
                {"char_field", "char(40)"},
                {"varchar_field", "varchar(100)"},
                {"date_field", "date"},
                {"timestamp_field", "timestamp"},
            };

            var columns = elements.Where(x => x.Collection.Name.Equals("test_data_types")).ToArray();
            Assert.Equal(14, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, (IDictionary<string, string>)dataTypes);
                Assert.Equal(dataTypes[column.Name], column.DbDataType);
            }
        }

        [Fact]
        public void ComplexDataTypesTest()
        {
            var (_, elements) = _instance.GetSchema(_container);

            var dataTypes = new Dictionary<string, string>() {
                {"id", "int"},
                {"int_array", "array<int>"},
                {"string_array", "array<string>"},
                {"int_map", "map<int,string>"},
                {"string_map", "map<string,string>"},
                {"struct_field.id", "int"},
                {"struct_field.name", "string"},
                {"struct_field.description", "string"}
            };

            var columns = elements.Where(x => x.Collection.Name.Equals("test_complex_types")).ToArray();
            Assert.Equal(8, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, (IDictionary<string, string>)dataTypes);
                Assert.Equal(dataTypes[column.Name], column.DbDataType);
            }
        }

        [Fact]
        public void SpecialFieldNamesTest()
        {
            var (_, elements) = _instance.GetSchema(_container);

            var fieldNames = new [] { "id", "low_case", "upcase", "camelcase", "table", "array", "select" };

            var columns = elements.Where(x => x.Collection.Name.Equals("test_field_names")).ToArray();
            Assert.Equal(fieldNames.Length, columns.Length);

            foreach (var column in columns)
            {
                Assert.Contains(column.Name, fieldNames);
            }
        }

        [Fact]
        public void CollectSampleTest()
        {
            var entity = new DataEntity("struct_field.name", DataType.String, "string", _container,
                new DataCollection(_container, "test_complex_types"));

            var samples = _instance.CollectSample(entity, 2);
            Assert.Equal(2, samples.Count);
            Assert.Contains("user", samples);
        }

        [Fact]
        public void GetDataCollectionMetricsTest()
        {
            var metrics = new DataCollectionMetrics[] {
                new DataCollectionMetrics()
                    {Name = "test_data_types", RowCount = 1, TotalSpaceKB = 0.107M},
                new DataCollectionMetrics()
                    {Name = "test_field_names", RowCount = 1, TotalSpaceKB = 0.014M},
                new DataCollectionMetrics()
                    {Name = "test_complex_types", RowCount = 6, TotalSpaceKB = 0.5M},
            };

            var result = _instance.GetDataCollectionMetrics(_container);
            Assert.Equal(3, result.Count);

            foreach (var metric in metrics)
            {
                var resultMetric = result.Find(x => x.Name.Equals(metric.Name));
                Assert.NotNull(resultMetric);

                Assert.Equal(metric.RowCount, resultMetric.RowCount);
                Assert.Equal(metric.TotalSpaceKB, resultMetric.TotalSpaceKB, 1);
            }
        }

    }
}
