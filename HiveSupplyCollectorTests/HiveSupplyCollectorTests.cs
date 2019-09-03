using System;
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



    }
}
