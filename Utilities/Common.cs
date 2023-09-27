  using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
using System.Linq;
using System.Text.RegularExpressions;

namespace Utilities
{
    public struct ProcessInfo
    {
        public string Id { get; }
        public string Type { get; }
        public string Url { get; }

        public ProcessInfo(string id, string type, string url)
        {
            Id = id;
            Type = type;
            Url = url;
        }
    }

    public struct ProcessState
    {
        public bool Crashed { get; }
        public bool Suspected { get; }

        public ProcessState(bool crashed, bool suspected)
        {
            this.Crashed = crashed;
            this.Suspected = suspected;
        }
    }

    public struct TkvConfig
    {
        public List<ProcessInfo> Clients { get; }
        public List<ProcessInfo> TransactionManagers { get; }
        public List<ProcessInfo> LeaseManagers { get; }
        public int NumberOfProcesses { get; }
        public (int, TimeSpan) TimeSlot { get; }

        public Dictionary<int, ProcessState>[] ProcessStates { get; }

        public TkvConfig(List<ProcessInfo> clients, List<ProcessInfo> transactionManagers, List<ProcessInfo> leaseManagers, int numberOfProcesses, int slotDuration, TimeSpan startTime, Dictionary<int, ProcessState>[] processStates)
        {
            this.Clients = clients;
            this.TransactionManagers = transactionManagers;
            this.LeaseManagers = leaseManagers;
            this.NumberOfProcesses = numberOfProcesses;
            this.TimeSlot = (slotDuration, startTime);
            this.ProcessStates = processStates;
        }

    }

    public static class Common
    {
        // TODO
        public static string GetSolutionDir()
        {
            return Directory.GetParent(AppDomain.CurrentDomain.BaseDirectory).Parent?.Parent?.Parent?.Parent?.FullName;
        }

        public static TkvConfig ReadConfig()
        {
            string configPath = Path.Join(GetSolutionDir(), "Launcher", "config.txt");
            string[] commands;
            try {
                commands = File.ReadAllLines(configPath); 
            } catch (FileNotFoundException e)
            {
                Console.WriteLine("Config file not found.");
                throw e;
            }

            int slotDuration = -1;
            TimeSpan startTime = new TimeSpan();
            Dictionary<int, ProcessState>[] processStates = null; // TODO: ??? array of dicts??
            List<ProcessInfo> clients = new List<ProcessInfo>();
            List<ProcessInfo> transactionManagers = new List<ProcessInfo>();
            List<ProcessInfo> leaseManagers = new List<ProcessInfo>();

            var rg = new Regex(@"(\([^0-9]*\d+[^0-9]*\))");

            foreach (string command in commands)
            {
                string[] args = command.Split(" ");

                if (args[0].Equals("P") && !args[2].Equals("C"))
                {
                    string processId = args[1];
                    ProcessInfo processInfo = new ProcessInfo(processId, args[2], args[3]);
                    switch (args[2])
                    {
                        case "C":
                            clients.Add(processInfo);
                            break;
                        case "T":
                            transactionManagers.Add(processInfo);
                            break;
                        case "L":
                            leaseManagers.Add(processInfo);
                            break;
                        default:
                            Console.WriteLine("Invalid process type.");
                            break;
                    }
                }

                else if (args[0].Equals("T"))
                {
                    string[] time = args[1].Split(":");
                    startTime = new TimeSpan(int.Parse(time[0]), int.Parse(time[1]), int.Parse(time[2]));
                }

                else if (args[0].Equals("D"))
                {
                    slotDuration = int.Parse(args[1]);
                }

                else if (args[0].Equals("S"))
                {
                    int numberOfSlots = int.Parse(args[1]);
                    processStates = new Dictionary<int, ProcessState>[numberOfSlots];
                }

                else if (args[0].Equals("F"))
                {
                    if (processStates == null)
                    {
                        continue;
                    }

                    // TODO: check if this is aight
                    MatchCollection matched = rg.Matches(command);
                    int slotId = int.Parse(args[1]);
                    processStates[slotId - 1] = new Dictionary<int, ProcessState>();

                    foreach (var match in matched.Cast<Match>())
                    {
                        string[] values = match.Value.Split(",");
                        var processId = int.Parse(values[0].Remove(0, 1));
                        var crashed = values[1].Equals(" C");
                        var suspected = values[2].Remove(values[2].Length - 1).Equals(" S");
                        // processStates[slotId - 1].Add(processId, new ProcessState(Crashed, Suspected));
                    }
                }
            }
            return new TkvConfig(clients, transactionManagers, leaseManagers, transactionManagers.Count + leaseManagers.Count, slotDuration, startTime, processStates);
        }
    }

}