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


    }
}
