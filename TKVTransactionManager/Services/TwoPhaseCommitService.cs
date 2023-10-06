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

        //public override Task<TentativeReply> Tentative(TentativeRequest request, ServerCallContext context)
        //{
        //    return Task.FromResult(tmService.Tentative(request));
        //}

        //public override Task<CommitReply> Commit(CommitRequest request, ServerCallContext context)
        //{
        //    return Task.FromResult(tmService.Commit(request));
        //}

        //public override Task<ListPendingRequestsReply> ListPendingRequests(ListPendingRequestsRequest request, ServerCallContext context)
        //{
        //    return Task.FromResult(tmService.ListPendingRequests(request));
        //}
    }
}