using TransactionManagerTransactionManagerProto;
using System.Data;
using System.Globalization;

namespace TKVTransactionManager.Services
{
    public class TMService
    {
        // Config file variables
        private readonly int processId;
        private readonly List<bool> processCrashedPerSlot; // TODO: rename to processe**s**
        private readonly List<Dictionary<int, bool>> processesSuspectedPerSlot;
        private readonly Dictionary<int, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers;
        // private readonly Dictionary<int, CompareAndSwap.CompareAndSwapClient> leaseManagers; // TODO: fix the service / see if its correct

        // Paxos variables
        private bool isCrashed;
        private int totalSlots;   // The number of total slots elapsed since the beginning of the program
        private int currentSlot;  // The number of experienced slots (process may be frozen and not experience all slots)
        private readonly Dictionary<int, int> primaryPerSlot;

        // Replication variables
        private decimal balance;
        private bool isCleanning;
        private int currentSequenceNumber;
        //private readonly Dictionary<(int, int), ClientCommand> tentativeCommands; // key: (clientId, clientSequenceNumber)
        //private readonly Dictionary<(int, int), ClientCommand> committedCommands;

        public TMService(
            int processId,
            List<bool> processCrashedPerSlot,
            List<Dictionary<int, bool>> processesSuspectedPerSlot,
            Dictionary<int, TwoPhaseCommit.TwoPhaseCommitClient> transactionManagers
            // Dictionary<int, CompareAndSwap.CompareAndSwapClient> leaseManagers
            )
        {
            this.processId = processId;
            this.transactionManagers = transactionManagers;
            //this.leaseManagers = leaseManagers;
            this.processCrashedPerSlot = processCrashedPerSlot;
            this.processesSuspectedPerSlot = processesSuspectedPerSlot;

            this.balance = 0;
            this.isCrashed = false;
            this.totalSlots = 0;
            this.currentSlot = 0;
            this.currentSequenceNumber = 0;
            this.primaryPerSlot = new Dictionary<int, int>();

            this.isCleanning = false;
            //this.tentativeCommands = new Dictionary<(int, int), ClientCommand>(); // TODO: client commands
            //this.committedCommands = new Dictionary<(int, int), ClientCommand>();
        }
        // TODO : etc...
    }
}