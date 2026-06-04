package sf.gateway.payment.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class UsageServiceGrpc {

  private UsageServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "sf.gateway.payment.v1.UsageService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<sf.gateway.payment.v1.Gateway.ReportRequest,
      sf.gateway.payment.v1.Gateway.ReportResponse> getReportMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Report",
      requestType = sf.gateway.payment.v1.Gateway.ReportRequest.class,
      responseType = sf.gateway.payment.v1.Gateway.ReportResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<sf.gateway.payment.v1.Gateway.ReportRequest,
      sf.gateway.payment.v1.Gateway.ReportResponse> getReportMethod() {
    io.grpc.MethodDescriptor<sf.gateway.payment.v1.Gateway.ReportRequest, sf.gateway.payment.v1.Gateway.ReportResponse> getReportMethod;
    if ((getReportMethod = UsageServiceGrpc.getReportMethod) == null) {
      synchronized (UsageServiceGrpc.class) {
        if ((getReportMethod = UsageServiceGrpc.getReportMethod) == null) {
          UsageServiceGrpc.getReportMethod = getReportMethod =
              io.grpc.MethodDescriptor.<sf.gateway.payment.v1.Gateway.ReportRequest, sf.gateway.payment.v1.Gateway.ReportResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Report"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  sf.gateway.payment.v1.Gateway.ReportRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  sf.gateway.payment.v1.Gateway.ReportResponse.getDefaultInstance()))
              .setSchemaDescriptor(new UsageServiceMethodDescriptorSupplier("Report"))
              .build();
        }
      }
    }
    return getReportMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static UsageServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<UsageServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<UsageServiceStub>() {
        @java.lang.Override
        public UsageServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new UsageServiceStub(channel, callOptions);
        }
      };
    return UsageServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static UsageServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<UsageServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<UsageServiceBlockingV2Stub>() {
        @java.lang.Override
        public UsageServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new UsageServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return UsageServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static UsageServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<UsageServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<UsageServiceBlockingStub>() {
        @java.lang.Override
        public UsageServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new UsageServiceBlockingStub(channel, callOptions);
        }
      };
    return UsageServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static UsageServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<UsageServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<UsageServiceFutureStub>() {
        @java.lang.Override
        public UsageServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new UsageServiceFutureStub(channel, callOptions);
        }
      };
    return UsageServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void report(sf.gateway.payment.v1.Gateway.ReportRequest request,
        io.grpc.stub.StreamObserver<sf.gateway.payment.v1.Gateway.ReportResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getReportMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service UsageService.
   */
  public static abstract class UsageServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return UsageServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service UsageService.
   */
  public static final class UsageServiceStub
      extends io.grpc.stub.AbstractAsyncStub<UsageServiceStub> {
    private UsageServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected UsageServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new UsageServiceStub(channel, callOptions);
    }

    /**
     */
    public void report(sf.gateway.payment.v1.Gateway.ReportRequest request,
        io.grpc.stub.StreamObserver<sf.gateway.payment.v1.Gateway.ReportResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getReportMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service UsageService.
   */
  public static final class UsageServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<UsageServiceBlockingV2Stub> {
    private UsageServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected UsageServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new UsageServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public sf.gateway.payment.v1.Gateway.ReportResponse report(sf.gateway.payment.v1.Gateway.ReportRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getReportMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service UsageService.
   */
  public static final class UsageServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<UsageServiceBlockingStub> {
    private UsageServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected UsageServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new UsageServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public sf.gateway.payment.v1.Gateway.ReportResponse report(sf.gateway.payment.v1.Gateway.ReportRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getReportMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service UsageService.
   */
  public static final class UsageServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<UsageServiceFutureStub> {
    private UsageServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected UsageServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new UsageServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<sf.gateway.payment.v1.Gateway.ReportResponse> report(
        sf.gateway.payment.v1.Gateway.ReportRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getReportMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_REPORT = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AsyncService serviceImpl;
    private final int methodId;

    MethodHandlers(AsyncService serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_REPORT:
          serviceImpl.report((sf.gateway.payment.v1.Gateway.ReportRequest) request,
              (io.grpc.stub.StreamObserver<sf.gateway.payment.v1.Gateway.ReportResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  public static final io.grpc.ServerServiceDefinition bindService(AsyncService service) {
    return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
        .addMethod(
          getReportMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              sf.gateway.payment.v1.Gateway.ReportRequest,
              sf.gateway.payment.v1.Gateway.ReportResponse>(
                service, METHODID_REPORT)))
        .build();
  }

  private static abstract class UsageServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    UsageServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return sf.gateway.payment.v1.Gateway.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("UsageService");
    }
  }

  private static final class UsageServiceFileDescriptorSupplier
      extends UsageServiceBaseDescriptorSupplier {
    UsageServiceFileDescriptorSupplier() {}
  }

  private static final class UsageServiceMethodDescriptorSupplier
      extends UsageServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    UsageServiceMethodDescriptorSupplier(java.lang.String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (UsageServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new UsageServiceFileDescriptorSupplier())
              .addMethod(getReportMethod())
              .build();
        }
      }
    }
    return result;
  }
}
