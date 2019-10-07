using System;
using System.IO;
using System.Text;
using Hive2;
using S2.BlackSwan.SupplyCollector.Models;
using SupplyCollectorDataLoader;

namespace HiveSupplyCollectorLoader
{
    public class HiveSupplyCollectorLoader : SupplyCollectorDataLoaderBase
    {
        private const string PREFIX = "hive2://";
        private string _database;

        private Connection Connect(string connectString)
        {
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

        public override void InitializeDatabase(DataContainer dataContainer) {
            
        }

        public override void LoadSamples(DataEntity[] dataEntities, long count) {
            using (var conn = Connect(dataEntities[0].Container.ConnectionString)) {
                var cursor = conn.GetCursor();

                cursor.Execute($"use {_database}");

                var sb = new StringBuilder();
                sb.Append("CREATE TABLE ");
                sb.Append(dataEntities[0].Collection.Name);
                sb.Append(" (\n");
                sb.Append("id_field int");

                foreach (var dataEntity in dataEntities)
                {
                    sb.Append(",\n");
                    sb.Append(dataEntity.Name.ToLower());
                    sb.Append(" ");

                    switch (dataEntity.DataType)
                    {
                        case DataType.String:
                            sb.Append("string");
                            break;
                        case DataType.Int:
                            sb.Append("int");
                            break;
                        case DataType.Double:
                            sb.Append("double");
                            break;
                        case DataType.Boolean:
                            sb.Append("boolean");
                            break;
                        case DataType.DateTime:
                            sb.Append("timestamp");
                            break;
                        default:
                            sb.Append("int");
                            break;
                    }

                    sb.AppendLine();
                }

                sb.Append(")");
                cursor.Execute(sb.ToString());

                var r = new Random();
                long rows = 0;
                while (rows < count)
                {
                    long bulkSize = 10000;
                    if (bulkSize + rows > count)
                        bulkSize = count - rows;

                    sb = new StringBuilder();
                    sb.Append("INSERT INTO ");
                    sb.Append(dataEntities[0].Collection.Name);
                    sb.Append("( id_field");
                    
                    foreach (var dataEntity in dataEntities)
                    {
                        sb.Append(", ");
                        sb.Append(dataEntity.Name.ToLower());
                    }
                    sb.Append(") VALUES ");

                    for (int i = 0; i < bulkSize; i++)
                    {
                        if (i > 0)
                            sb.Append(", ");

                        sb.Append("(");
                        sb.Append(rows + i);
                        foreach (var dataEntity in dataEntities)
                        {
                            sb.Append(", ");

                            switch (dataEntity.DataType)
                            {
                                case DataType.String:
                                    sb.Append("'");
                                    sb.Append(new Guid().ToString());
                                    sb.Append("'");
                                    break;
                                case DataType.Int:
                                    sb.Append(r.Next().ToString());
                                    break;
                                case DataType.Double:
                                    sb.Append(r.NextDouble().ToString().Replace(",", "."));
                                    break;
                                case DataType.Boolean:
                                    sb.Append(r.Next(100) > 50 ? "true" : "false");
                                    break;
                                case DataType.DateTime:
                                    var val = DateTimeOffset
                                        .FromUnixTimeMilliseconds(
                                            DateTimeOffset.Now.ToUnixTimeMilliseconds() + r.Next()).DateTime;
                                    sb.Append("'");
                                    sb.Append(val.ToString("s"));
                                    sb.Append("'");
                                    break;
                                default:
                                    sb.Append(r.Next().ToString());
                                    break;
                            }
                        }

                        sb.Append(")");
                    }

                    cursor.Execute(sb.ToString());

                    rows += bulkSize;
                    Console.Write(".");
                }

                Console.WriteLine();
            }
        }

        public override void LoadUnitTestData(DataContainer dataContainer) {
            using (var conn = Connect(dataContainer.ConnectionString)) {
                var cursor = conn.GetCursor();

                using (var reader = new StreamReader("tests/data.sql"))
                {
                    var sb = new StringBuilder();
                    while (!reader.EndOfStream)
                    {
                        var line = reader.ReadLine();
                        if (String.IsNullOrEmpty(line))
                            continue;

                        sb.AppendLine(line);
                        if (line.TrimEnd().EndsWith(";")) {
                            Console.WriteLine(sb.ToString());
                            cursor.Execute(sb.ToString().TrimEnd(new[] {'\n', '\r', '\t', ' ', ';'}));
                            sb.Clear();
                        }
                    }
                }
            }
        }
    }
}
