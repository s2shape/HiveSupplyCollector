using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using Hive2;
using S2.BlackSwan.SupplyCollector;
using S2.BlackSwan.SupplyCollector.Models;
using Thrift.Protocol;

namespace HiveSupplyCollector
{
    public class HiveSupplyCollector : SupplyCollectorBase {
        private string _database;

        public override List<string> DataStoreTypes() {
            return (new[] { "Hive" }).ToList();
        }

        private const string PREFIX = "hive2://";

        public string BuildConnectionString(string host, int port, string user, string password, string database) {
            return $"{PREFIX}{user}:{password}@{host}:{port}/{database}";
        }

        public override List<string> CollectSample(DataEntity dataEntity, int sampleSize) {
            throw new NotImplementedException();
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            throw new NotImplementedException();
        }

        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            throw new NotImplementedException();
        }

        private Connection Connect(string connectString) {
            if (!connectString.StartsWith(PREFIX))
                throw new ArgumentException("Invalid connection string!");

            var userIndex = PREFIX.Length;
            var passwordIndex = connectString.IndexOf(":", userIndex);
            if (passwordIndex <= 0)
                throw new ArgumentException("Invalid connection string!");
            var hostIndex = connectString.IndexOf("@", passwordIndex);
            if (hostIndex <= 0)
                throw new ArgumentException("Invalid connection string!");
            var portIndex = connectString.IndexOf(":", hostIndex);
            if (portIndex <= 0)
                throw new ArgumentException("Invalid connection string!");
            var dbIndex = connectString.IndexOf("/", portIndex);
            if (dbIndex <= 0)
                throw new ArgumentException("Invalid connection string!");

            var user = connectString.Substring(userIndex, passwordIndex - userIndex);
            var password = connectString.Substring(passwordIndex + 1, hostIndex - passwordIndex - 1);
            var host = connectString.Substring(hostIndex + 1, portIndex - hostIndex - 1);
            var port = Int32.Parse(connectString.Substring(portIndex + 1, dbIndex - portIndex - 1));
            _database = connectString.Substring(dbIndex + 1);

            return new Connection(host, port, user, password, TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6);
        }

        public override bool TestConnection(DataContainer container) {
            try {
                using (var conn = Connect(container.ConnectionString)) {
                    var cursor = conn.GetCursor();
                    cursor.Execute($"use {_database}");
                }

                return true;
            }
            catch (Exception) {
                return false;
            }
        }
    }
}
