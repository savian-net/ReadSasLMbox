using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.OleDb;
using System.Drawing;
using System.Text;
using MimeKit;
using static System.Console;
using static System.Runtime.InteropServices.JavaScript.JSType;
using static System.Data.Entity.Infrastructure.Design.Executor;
using System;
using System.CodeDom;
using System.Data.SQLite;
using System.Data.Common;
using Org.BouncyCastle.Math;

namespace ReadSasLMbox
{
    public class SqliteManager
    {
        SqliteConnection conn;
        private string _tableName = "Messages";
        private List<string> _columns = new List<string>();
        private string WorkArea;
        private string DbName;
        private List<MimeMessage> Errors = new();

        internal void Process(string workDir, List<MimeMessage> recs)
        {
            WorkArea = workDir;
            DbName = "SASL";
            WriteLine("Creating SQLite process");
            CreateConnection();
            CreateTable();
            PopulateData(recs);
        }

        private void PopulateData(List<MimeMessage> recs)
        {
            WriteLine("Adding data to table");
            WriteLine($"Adding data. Total rows {recs.Count}");
            conn.Open();

            int i = 0;
            using (var transaction = conn.BeginTransaction())
            {
                var cmd = conn.CreateCommand();

                cmd.CommandText = $"INSERT INTO {_tableName} (ID, FromEmail, Subject, DateTime, Body) " +
                                  "VALUES ($ID, $FromEmail, $Subject, $Date, $BodyText)";
                cmd.CommandType = CommandType.Text;

                var paramId = cmd.Parameters.Add("$ID", SqliteType.Integer);
                var paramFromEmail = cmd.Parameters.Add("$FromEmail", SqliteType.Text);
                var paramSubject = cmd.Parameters.Add("$Subject", SqliteType.Text);
                var paramDate = cmd.Parameters.Add("$Date", SqliteType.Text);
                var paramBody = cmd.Parameters.Add("$BodyText", SqliteType.Text);

                foreach (var rec in recs)
                {
                    try
                    {
                        var fromEmail = rec.From.Mailboxes?.FirstOrDefault()?.Address;
                        if (fromEmail == null)
                        {
                            fromEmail = rec.MessageId;
                        }
                        paramId.Value = i;
                        paramFromEmail.Value = fromEmail;
                        paramSubject.Value = rec.Subject ?? "N/A";
                        if (string.IsNullOrEmpty(rec.Subject) || string.IsNullOrEmpty(rec.TextBody))
                        {
                            Errors.Add(rec);
                            continue;
                        }
                        paramDate.Value = rec.Date.ToString("yyyy-MM-ddThh:mm:ss");
                        paramBody.Value = rec.TextBody.Trim();

                        cmd.ExecuteNonQuery();
                        if (i % 10000 == 0)
                        {
                            WriteLine($"Processed: {i}");
                        }
                        i++;
                    }
                    catch (Exception ex)
                    {
                        WriteLine(ex);
                        Errors.Add(rec);
                    }
                }
                transaction.Commit();
            }

            WriteLine($"Total errors: {Errors.Count}");
            WriteLine($"Total records added: {i}");
            WriteLine("Finished adding data to table");
            conn.Close();
        }

        private void CreateTable()
        {
            WriteLine("Creating SQL for table creation");
            var sb = new StringBuilder();
            SqliteCommand cmd = new SqliteCommand();
            if (TableExists(_tableName))
            {
                WriteLine("Creating SQLite table");
                conn.Open();
                cmd.CommandText = " DROP Table 'Messages'";
                conn.Close();
                cmd.ExecuteNonQuery(); 
                WriteLine("Table deleted");
            }
            sb.AppendLine($@"CREATE TABLE {_tableName}(");
            sb.AppendLine($@"   ID INTEGER PRIMARY KEY,");
            sb.Append($@"   'FromEmail' TEXT NULL,");
            sb.Append($@"   'DateTime' TEXT NULL,");
            sb.Append($@"   'Subject' TEXT NULL,");
            sb.Append($@"   'Body' TEXT NULL");
            sb.AppendLine($@"  ); ");
            conn.Open();
            cmd = new SqliteCommand(sb.ToString(), conn);
            cmd.ExecuteNonQuery();
            conn.Close();
            WriteLine("Finished creating SQL for table creation");
        }

        private void AddData(DataTable data)
        {
            WriteLine($"Adding data. Total rows {data.Rows.Count}", Color.LightBlue);
            conn.Open();
            using (var txn = conn.BeginTransaction())
            {
                var sb = new StringBuilder();
                //var columns = _columns.SplitIntoDelimitedGroups(3, ",");
                //sb.AppendLine($"INSERT INTO {_tableName}(");
                //for (int i = 0; i < columns.Count; i++)
                //{
                //    if (i < columns.Count - 1)
                //    {
                //        sb.AppendLine($"{columns[i].ToUpper()},");
                //    }
                //    else
                //    {
                //        sb.AppendLine($"{columns[i].ToUpper()}");
                //    }
                //}

                sb.Length--;
                sb.AppendLine($")");
                sb.AppendLine($"VALUES");
                for (int r = 0; r < data.Rows.Count; r++)
                {
                    var dataRow = data.Rows[r];
                    sb.Append($"(");

                    var rec = new StringBuilder();
                    for (int c = 0; c < data.Columns.Count; c++)
                    {
                        if (dataRow[c] is string)
                        {
                            var value = dataRow[c].ToString();
                            if (string.IsNullOrEmpty(value))
                            {
                                rec.Append($"'',");
                                continue;
                            }

                            rec.Append($"'{dataRow[c].ToString().Replace("'", "''")}',");
                        }
                        else
                        {
                            var value = dataRow[c];
                            if (value.ToString() == string.Empty)
                            {
                                value = "NULL";
                            }

                            rec.Append($"{value},");
                        }
                    }

                    rec.Length--;
                    sb.Append(rec.ToString());
                    sb.AppendLine($"),");
                }

                //                sb.AppendLine($")");
                var sql = sb.ToString().TrimEnd().TrimEnd(',');
                WriteLine("Executing SQL command");
                SqliteCommand cmd = new SqliteCommand();
                cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
                txn.Commit();
            }

            conn.Close();
        }

        private void CreateConnection()
        {
            var db = Path.Combine(WorkArea, $"{DbName}.sqlite");
            SQLiteConnection.CreateFile(db);
            conn = new SqliteConnection($"Data Source={db}");
        }

        //private string CreateTableSql(List<SasSchema> meta)
        //{
        //    WriteLine("Creating SQL for table creation", Color.LightBlue);

        //    _columns.Clear();
        //    var sb = new StringBuilder();
        //    _tableName = meta[0].TableName;
        //    sb.AppendLine($@"CREATE TABLE {_tableName}(");
        //    sb.AppendLine($@"   ID INTEGER PRIMARY KEY AUTOINCREMENT ,");
        //    WriteLine($"Found {meta.Count} columns", Color.LightBlue);
        //    for (int i = 0; i < meta.Count; i++)
        //    {
        //        var sasVar = meta[i];
        //        sb.Append($@"   '{sasVar.ColumnName.ToUpper()}' {GetDataType(sasVar.DataType)} NULL");
        //        _columns.Add($"{sasVar.ColumnName}");
        //        if (i != meta.Count - 1)
        //        {
        //            sb.AppendLine(",");
        //        }
        //        else
        //        {
        //            sb.AppendLine();
        //        }
        //    }

        //    sb.AppendLine($@"  ); ");
        //    WriteLine("Finished creating SQL for table creation", Color.LightBlue);
        //    return sb.ToString();
        //}

        private string GetDataType(OleDbType dataType)
        {
            switch (dataType)
            {
                case OleDbType.Char:
                    return "TEXT";
                case OleDbType.Double:
                    return "REAL";
                default:
                    return "TEXT";
            }
        }

        public void CreateTable(string sql, DataTable data)
        {
            conn.Open();
            var cmd = new SqliteCommand(sql, conn);
            cmd.ExecuteNonQuery();
            conn.Close();
        }

        private bool TableExists(string tableName)
        {
            bool exists;
            conn.Open();

            try
            {
                var cmd = new SqliteCommand(
                    "select case when exists((select * from information_schema.tables where table_name = '" +
                    tableName + "')) then 1 else 0 end", conn);
                exists = (int)cmd.ExecuteScalar() == 1;
            }
            catch
            {
                try
                {
                    exists = true;
                    var cmdOthers = new SqliteCommand("select 1 from " + tableName + " where 1 = 0", conn);
                    cmdOthers.ExecuteNonQuery();
                }
                catch
                {
                    exists = false;
                }
            } 

            conn.Close();
            return exists;
        }
    }
}

