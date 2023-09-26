using Utilities;

namespace TKVClient
{
    internal class Program
    {
        static void wait(string[] command)
        {
            try
            {
                if (command.Length == 2)
                {
                    Console.WriteLine("Waiting for " + command[1] + " milliseconds...");
                    System.Threading.Thread.Sleep(int.Parse(command[1]));
                }
                else { Console.WriteLine("No time amount provided for wait."); }
            }
            catch (FormatException e)
            {
                Console.WriteLine("Invalid time amount provided for wait.");
            }
        }

        static void transactionRequest(string[] command)
        {
            if (command.Length == 3)
            {
                string[] reads = command[1].Split(',', StringSplitOptions.RemoveEmptyEntries);
                string[] writes = command[2].Split(',', StringSplitOptions.RemoveEmptyEntries);

                foreach (string read in reads)
                {
                    Console.WriteLine("DADINT: [" + read + "]");
                }
                foreach (string write in writes)
                {
                    try
                    {
                        string[] writePair = write.Split(':', StringSplitOptions.RemoveEmptyEntries); // TODO??
                        if (writePair.Length != 2)
                        {
                            Console.WriteLine("Invalid write pair provided for transaction request.");
                            return;
                        }
                        int.Parse(writePair[1]);
                        Console.WriteLine("DADINT: [" + writePair[0] + ", " + writePair[1] + "]");
                    } catch (FormatException e)
                    {
                        Console.WriteLine("Invalid write pair provided for transaction request.");
                    }
                }
            }
            else { Console.WriteLine("Invalid number of arguments provided for transaction request."); }
        }

        static bool handleCommand(string command)
        {
            string[] commandArgs = command.Split(' ', StringSplitOptions.RemoveEmptyEntries);

            if (command.Length == 0) { 
                Console.WriteLine("No command provided.");
                return true;
            }

            switch (commandArgs[0].ToLower())
            {
                case "w":
                    Console.WriteLine("Client set to wait...");
                    wait(commandArgs);
                    break;
                case "t":
                    Console.WriteLine("Processing transaction request...");
                    transactionRequest(commandArgs);
                    break;
                case "x":
                    Console.WriteLine("Sending status request...");
                    // TODO: send status request
                    break;
                case "q":
                    Console.WriteLine("Closing client...");
                    return false;
                default:
                    Console.WriteLine("Invalid command.");
                    break;
            }
            return true;
        }

        static void Main(string[] args)
        {
            AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

            // Command Line Arguments
            if (args.Length < 2)
            {
                Console.WriteLine("You need to pass at least 2 arguments, processId and scriptName");
                return;
            }
            string processId = args[0];
            string scriptName = args[1]; // TODO: place elsewhere?? launcher??
            bool debug = args.Length > 2 && args[2].Equals("debug");

            Console.WriteLine($"TKVClient with id ({processId}) starting...");

            TKVConfig config;
            try { config = Common.ReadConfig(); }
            catch (Exception e)
            {
                Console.WriteLine("Error reading config file.");
                return;
            }
            // TODO: process config file
            (int slotDuration, TimeSpan startTime) = config.TimeSlot;

            // Read client scripts
            string baseDirectory = Common.GetSolutionDir();
            string scriptFilePath = Path.Join(baseDirectory, "TKVClient", "Scripts", scriptName + ".txt");
            Console.WriteLine("Using script (" + scriptFilePath +") for TKVClient.");

            string[] commands;
            try { commands = File.ReadAllLines(scriptFilePath); }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("Script file not found.");
                return;
            }

            // Wait for slots to start
            if (DateTime.Now.TimeOfDay < startTime)
            {
                System.Threading.Thread.Sleep(startTime - DateTime.Now.TimeOfDay);
            }

            int clientTimestamp = 0;

            foreach (string command in commands) { handleCommand(command); }
        }
    }
}