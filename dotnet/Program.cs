﻿using System.Threading.Tasks;
using Spectre.Console.Cli;

namespace KafkaTool
{
    public static class Program
    {
        static async Task Main(string[] args)
        {
            var app = new CommandApp();

            app.Configure(c =>
            {
                c.AddCommand<Producer1>("producer1");
                c.AddCommand<Consumer1>("consumer1");
            });

            await app.RunAsync(args);
        }
    }
}