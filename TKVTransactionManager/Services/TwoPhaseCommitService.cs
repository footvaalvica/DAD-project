using Grpc.Core;
using System.Threading.Tasks;
using TransactionManagerTransactionManagerProto; // TODO: why this necessary :thinking:

namespace TKVTransactionManager.Services
{
    public class TwoPhaseCommitService : TwoPhaseCommit.TwoPhaseCommitBase
    {
        private readonly TMService tmService;

        public TwoPhaseCommitService(TMService tmService)
        {
            this.tmService = tmService;
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