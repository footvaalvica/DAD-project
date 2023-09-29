using TransactionManagerTransactionManagerProto;
using TransactionManagerLeaseManagerServiceProto;
using System.Data;
using System.Globalization;
using ClientTransactionManagerProto;

namespace TKVTransactionManager.Services
{
    public class ServerService
    {
        // Config file variables
        private readonly string processId;
        private readonly List<bool> processCrashedPerSlot; // TODO: rename to processe**s**
        private readonly List<Dictionary<int, bool>> processesSuspectedPerSlot;
        private readonly Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers;
        private readonly Dictionary<string, CompareAndSwap.CompareAndSwapClient> leaseManagers; // TODO: fix the service / see if its correct

        // Paxos variables
        private bool isCrashed;
        private int totalSlots;   // The number of total slots elapsed since the beginning of the program
        private int currentSlot;  // The number of experienced slots (process may be frozen and not experience all slots)
        private readonly Dictionary<int, int> primaryPerSlot;

        // Replication variables
        private decimal balance;
        private bool isCleaning;
        private int currentSequenceNumber;
        //private readonly Dictionary<(int, int), ClientCommand> tentativeCommands; // key: (clientId, clientSequenceNumber)
        //private readonly Dictionary<(int, int), ClientCommand> committedCommands;

        public ServerService(
            string processId,
            //List<bool> processCrashedPerSlot,
            //List<Dictionary<int, bool>> processesSuspectedPerSlot,
            Dictionary<string, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers,
            Dictionary<string, CompareAndSwap.CompareAndSwapClient> leaseManagers
            )
        {
            this.processId = processId;
            this.transactionManagers = transactionManagers;
            this.leaseManagers = leaseManagers;
            //this.processCrashedPerSlot = processCrashedPerSlot;
            //this.processesSuspectedPerSlot = processesSuspectedPerSlot;

            this.balance = 0;
            this.isCrashed = false;
            this.totalSlots = 0;
            this.currentSlot = 0;
            this.currentSequenceNumber = 0;
            this.primaryPerSlot = new Dictionary<int, int>();

            this.isCleaning = false;
            //this.tentativeCommands = new Dictionary<(int, int), ClientCommand>(); // TODO: client commands
            //this.committedCommands = new Dictionary<(int, int), ClientCommand>();
        }
        // TODO : etc...

        public void PrepareSlot()
        {
            // TODO
        }

        public StatusResponse Status(StatusRequest statusRequest)
        {
            return new StatusResponse { Status = true };
        }
    }
}