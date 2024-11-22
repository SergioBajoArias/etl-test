using System;
using System.Data.SqlClient;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Paillave.Etl.Core;
using Paillave.Etl.SqlServer;


namespace com.sergio
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var processRunner = StreamProcessRunner.Create<string>(DefineProcess);

            using (var cnx = GetSqlConnection())
            {
                cnx.Open();
                var executionOptions = new ExecutionOptions<string>
                {
                    Resolver = new SimpleDependencyResolver().Register(cnx)
                };
                var res = await processRunner.ExecuteAsync(args[0], executionOptions);
                Console.WriteLine(JsonConvert.SerializeObject(res, Formatting.Indented));
            }

                
        }

        private static void DefineProcess(ISingleStream<string> contextStream)
        {
            contextStream
                .CrossApplySqlServerQuery("get jobs", o => o
                    .FromQuery("select j.* from dbo.Jobs as j")
                    .WithMapping<Job>())
                // .Do("show job on console", j => Console.WriteLine($"{j.Id} - {j.Name}"))
                .Do("uppercase job name", j => )
                .SqlServerSave("save in DB", job => job
                    .ToTable("dbo.Jobs_Target"));
        }

        private static SqlConnection GetSqlConnection()
        {
            IConfigurationRoot config = new ConfigurationBuilder()
                .AddUserSecrets<Program>()
                .Build();
            return new SqlConnection(config["ConnectionStrings:SqlServer"]);
        }
    }

    class Job
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public bool Active { get; set; }
        public string ClientId { get; set; }
    }    
}
