using System;
using System.Collections.Generic;
using System.Dynamic;
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
            var results = new List<string>();

            using (var conn = Connect(dataEntity.Container.ConnectionString))
            {
                var cursor = conn.GetCursor();
                cursor.Execute($"use {_database}");
                
                cursor.Execute($"select {dataEntity.Name} from {dataEntity.Collection.Name} distribute by rand() sort by rand() limit {sampleSize}");
                ExpandoObject row;
                while ((row = cursor.FetchOne()) != null)
                {
                    var rowValues = row as IDictionary<string, object>;

                    var columnName = rowValues.Keys.First();
                    results.Add(rowValues[columnName].ToString());
                }
            }

            return results;
        }

        public override List<DataCollectionMetrics> GetDataCollectionMetrics(DataContainer container) {
            throw new NotImplementedException();
        }

        private DataType ConvertDataType(string dbDataType)
        {
            if ("bigint".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("bool".Equals(dbDataType))
            {
                return DataType.Boolean;
            }
            else if ("int".Equals(dbDataType))
            {
                return DataType.Long;
            }
            else if ("smallint".Equals(dbDataType))
            {
                return DataType.Short;
            }
            else if ("decimal".Equals(dbDataType))
            {
                return DataType.Decimal;
            }
            else if ("tinyint".Equals(dbDataType))
            {
                return DataType.Byte;
            }
            else if ("char".Equals(dbDataType))
            {
                return DataType.Char;
            }
            else if ("varchar".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("string".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("ntext".Equals(dbDataType))
            {
                return DataType.String;
            }
            else if ("float".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("double".Equals(dbDataType))
            {
                return DataType.Double;
            }
            else if ("date".Equals(dbDataType))
            {
                return DataType.DateTime;
            }
            else if ("timestamp".Equals(dbDataType))
            {
                return DataType.DateTime;
            }

            //TODO: what about array<?> and map<?:?> types?

            return DataType.Unknown;
        }


        public override (List<DataCollection>, List<DataEntity>) GetSchema(DataContainer container) {
            var collections = new List<DataCollection>();
            var entities = new List<DataEntity>();

            using (var conn = Connect(container.ConnectionString))
            {
                var cursor = conn.GetCursor();
                cursor.Execute("show tables");
                ExpandoObject row;
                while ((row = cursor.FetchOne()) != null) {
                    var rowValues = row as IDictionary<string, object>;

                    var columnName = rowValues.Keys.First();
                    collections.Add(new DataCollection(container, rowValues[columnName].ToString()));
                }

                foreach (var collection in collections) {
                    cursor.Execute($"desc {collection.Name}");

                    while ((row = cursor.FetchOne()) != null)
                    {
                        var rowValues = row as IDictionary<string, object>;
                        
                        var columnName = rowValues["col_name"].ToString();
                        var dataType = rowValues["data_type"].ToString();

                        entities.Add(new DataEntity(columnName, ConvertDataType(dataType), dataType, container, collection));
                    }
                }
            }

            return (collections, entities);
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
