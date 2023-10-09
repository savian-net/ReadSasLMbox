
//using MimeKit;

using System.Data.SQLite;
using System.Diagnostics;
using System.IO;
using System.Net.Mail;
using System.Net.WebSockets;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using MimeKit;
using static System.Console;

namespace ReadSasLMbox
{
    /// <summary> 
    /// Based upon a discussion on SAS-L around September 2023 on how to get the SAS-L Archives into the public domain
    /// </summary>
    /// <remarks>
    /// Barry Grau said:
    ///      SAS-L and comp.soft-sys-sas usenet groups used to be gatewayed in both directions.
    ///      Go here: https://archive.org/download/usenet-comp
    ///      Scroll to comp.soft-sys.sas.mbox.zip(or just click on the link here)
    /// </remarks>
    /// 
    internal class Program
    {
        private static string dataFile = @"C:\scratch\comp.soft-sys.sas.mbox\comp.soft-sys.sas.mbox";
        //private static string dataFile = @"C:\scratch\comp.soft-sys.sas.mbox\Sample.mbox";
        private static string testFile = @"C:\scratch\test.txt";
        private static string errorsFile = @"C:\scratch\errors.txt";
        private static Regex rxMessage = new Regex(@"((From\s+-?\d+)$)", RegexOptions.Multiline);
        static SQLiteConnection conn;
        static void Main(string[] args)
        {
            Console.WriteLine("Parsing SAS-L messages...");
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();
            //ReadTest();
            //CreateConnection();
            //CreateTable();
            //InsertData();
            var recs = ReadDataFile();
            var dbMgr = new SqliteManager();
            dbMgr.Process(@"z:\scratch", recs);
            stopWatch.Stop();
            WriteLine($"Total time: {stopWatch.ElapsedMilliseconds} ms");
            // GetSqliteVersion();
            //ReadDataFile();
        }

        static SQLiteConnection CreateConnection()
        {

            // Create a new database connection:
            conn = new SQLiteConnection(@"Data Source='x:\scratch\SAS-L.db';Version=3;New=True;Compress=True;");
            try
            {
                conn.Open();
            }
            catch (Exception ex)
            {

            }
            return conn;
        }


        static void CreateTable()
        {

            SQLiteCommand sqlite_cmd;
            string Createsql = "CREATE TABLE Messages (Col1 VARCHAR(20), Col2 INT)";
            sqlite_cmd = conn.CreateCommand();
            sqlite_cmd.CommandText = Createsql;
            sqlite_cmd.ExecuteNonQuery();
        }

        static void InsertData()
        {
            SQLiteCommand sqlite_cmd;
            sqlite_cmd = conn.CreateCommand();
            sqlite_cmd.CommandText = "INSERT INTO Messages (Col1, Col2) VALUES('Test Text ', 1); ";
            sqlite_cmd.ExecuteNonQuery();

        }


        static void ReadTable()
        {
            SQLiteDataReader sqlite_datareader;
            SQLiteCommand sqlite_cmd;
            sqlite_cmd = conn.CreateCommand();
            sqlite_cmd.CommandText = "SELECT * FROM Messages";

            sqlite_datareader = sqlite_cmd.ExecuteReader();
            while (sqlite_datareader.Read())
            {
                string myreader = sqlite_datareader.GetString(0);
                Console.WriteLine(myreader);
            }
            conn.Close();
        }

        private static void GetSqliteVersion()
        {
            string cs = "Data Source=:memory:";
            string stm = "SELECT SQLITE_VERSION()";

            using var con = new SqliteConnection(cs);
            con.Open();

            using var cmd = new SqliteCommand(stm, con);
            string version = cmd.ExecuteScalar().ToString();

            WriteLine($"SQLite version: {version}");
        }

        private static void ReadTest()
        {
            var data = File.ReadAllText(testFile);
            var messages = data.SplitAndKeepDelimiter(@"\w+\:\s\d+");
        }


        private static List<MimeMessage> ReadDataFile()
        {
            var data = File.ReadAllText(dataFile);
            var recs = data.SplitAndKeepDelimiter(@"From\s+-?\d+");
            WriteLine($"Total messages: {recs.Count()}");
            var messages = new List<MimeMessage>();
            var errors = new List<(string Exception, string Message)>();
            foreach (var rec in recs)
            {
                MemoryStream mm = new MemoryStream(Encoding.ASCII.GetBytes(rec));
                try
                {
                    var msg = MimeKit.MimeMessage.Load(mm);
                    messages.Add(msg);
                }
                catch (Exception ex)
                {
                    errors.Add((Exception: ex.Message, Message: rec));
                }
            }

            using var sw = new StreamWriter(errorsFile);
            foreach (var err in errors)
            {
                sw.WriteLine(new string('=', 80));
                sw.WriteLine($"Exception: {err.Exception}");
                sw.WriteLine($"{err.Message}");
            }
            return messages;
        }

    }
}