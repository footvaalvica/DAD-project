using TransactionManagerTransactionManagerProto;

namespace TKVTransactionManager.Services
{
    public class TwoPhaseCommitService : TwoPhaseCommit.TwoPhaseCommitBase
    {
        private readonly ServerService serverService;

        public TwoPhaseCommitService(ServerService serverService)
        {
            this.serverService = serverService;
        }
    }
}