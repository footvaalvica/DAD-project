using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using TransactionManagerLeaseManagerServiceProto;

namespace TKVLeaseManager.Services
{
    public class RequestLeaseService : TransactionManager_LeaseManagerService.TransactionManager_LeaseManagerServiceBase
    {
        private readonly LeaseManagerService _leaseManagerService;

        public RequestLeaseService(LeaseManagerService leaseManagerService) {
            _leaseManagerService = leaseManagerService;
        }
        public override Task<Empty> Lease(LeaseRequest request, ServerCallContext context)
        {
            return Task.FromResult(_leaseManagerService.LeaseRequest(request));
        }
        public override Task<StatusUpdateResponse> StatusUpdate(Empty request, ServerCallContext context)
        {
            return Task.FromResult(_leaseManagerService.StatusUpdate());
        }
    }
}