using TransactionManagerTransactionManagerProto;

namespace TKVTransactionManager.Services
{
    public class GossipService : TwoPhaseCommit.TwoPhaseCommitBase
    {
        private readonly ServerService serverService;

        public GossipService(ServerService serverService)
        {
            this.serverService = serverService;
        }
    }
}