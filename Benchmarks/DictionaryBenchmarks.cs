using BenchmarkDotNet.Attributes;
using CachingFramework.Redis;
using CachingFramework.Redis.Contracts;
using CachingFramework.Redis.Serializers;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmarks
{
    public class DictionaryBenchmarks
    {
        public static string Config = "localhost:6379, allowAdmin=true";

        private const string Key = "UT_CacheDictionaryObjectAsync_Benchmark";


        // A context using json
        
        public readonly PrintableContext _jsonContext;
        // A context using msgpack
        public readonly PrintableContext _msgPackContext;

        public IEnumerable<PrintableContext> Contexts => new List<PrintableContext>
        {
            _jsonContext, _msgPackContext
        };

        [ParamsSource(nameof(Contexts))]
        public PrintableContext Context { get; set; }

        public DictionaryBenchmarks()
        {
            var configuration = new ConfigurationBuilder()
                                    .AddUserSecrets<DictionaryBenchmarks>()
                                    .Build();

            var p = configuration["Password"];
            if (!string.IsNullOrEmpty(p))
                Config += $",password={p}";

            _jsonContext = new PrintableContext(Config, new JsonSerializer());
            _msgPackContext = new PrintableContext(Config, new CachingFramework.Redis.MsgPack.MsgPackSerializer());
        }

        [GlobalSetup]
        public void Setup()
        {
            var dict = Context.Collections.GetRedisDictionary<string, User>(Key) ?? throw new InvalidOperationException("Failed to get the cache");
            var users = GetUsers();
            foreach(var user in users)
            {
                dict.Add(user.Id, user);
            }
        }

        [GlobalCleanup]
        public void Clean()
        {
            var dict = Context.Collections.GetRedisDictionary<string, User>(Key) ?? throw new InvalidOperationException("Failed to get the cache");
            var keys = dict.Keys;
            foreach(var key in keys)
            {
                dict.Remove(key);
            }
        }


        [Benchmark]
        public IList<string> GettingDictionary()
        {
            var dict = Context.Collections.GetRedisDictionary<string, User>(Key) ?? throw new InvalidOperationException("Failed to get the cache");
            var userIds = new List<string>();
            foreach((var userId, var value) in dict)
            {
                userIds.Add(value.Id + userId);
            }
            return userIds;
        }
        private List<User> GetUsers()
        {
            var loc1 = new Location()
            {
                Id = 1,
                Name = "one"
            };
            var loc2 = new Location()
            {
                Id = 2,
                Name = "two"
            };
            var user1 = new User()
            {
                Id = Guid.NewGuid().ToString(),
                Deparments = new List<Department>()
                {
                    new Department() {Id = 1, Distance = 123.45m, Size = 2, Location = loc1},
                    new Department() {Id = 2, Distance = 400, Size = 1, Location = loc2}
                }
            };
            var user2 = new User()
            {
                Id = Guid.NewGuid().ToString(),
                Deparments = new List<Department>()
                {
                    new Department() {Id = 3, Distance = 500, Size = 1, Location = loc2},
                    new Department() {Id = 4, Distance = 125.5m, Size = 3, Location = loc1}
                }
            };
            var user3 = new User()
            {
                Id = Guid.NewGuid().ToString(),
                Deparments = new List<Department>()
                {
                    new Department() {Id = 5, Distance = 5, Size = 5, Location = loc2},
                }
            };
            var user4 = new User()
            {
                Id = Guid.NewGuid().ToString(),
                Deparments = new List<Department>()
                {
                    new Department() {Id = 6, Distance = 100, Size = 10, Location = loc1},
                }
            };
            return new List<User>() { user1, user2, user3, user4 };
        }
    }



    public class PrintableContext : RedisContext
    {
        public PrintableContext()
        {
        }

        public PrintableContext(string configuration) : base(configuration)
        {
        }

        public PrintableContext(ConfigurationOptions configuration) : base(configuration)
        {
        }

        public PrintableContext(IConnectionMultiplexer connection) : base(connection)
        {
        }

        public PrintableContext(string configuration, ISerializer serializer) : base(configuration, serializer)
        {
        }

        public PrintableContext(ConfigurationOptions configuration, ISerializer serializer) : base(configuration, serializer)
        {
        }

        public PrintableContext(IConnectionMultiplexer connection, ISerializer serializer) : base(connection, serializer)
        {
        }

        public PrintableContext(string configuration, ISerializer serializer, TextWriter log) : base(configuration, serializer, log)
        {
        }

        public PrintableContext(ConfigurationOptions configuration, ISerializer serializer, TextWriter log) : base(configuration, serializer, log)
        {
        }

        public override string ToString()
        {
            return GetSerializer().GetType().Name;
        }
    }
}
