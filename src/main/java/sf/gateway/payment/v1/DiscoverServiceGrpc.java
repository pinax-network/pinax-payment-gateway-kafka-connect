package sf.gateway.payment.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class DiscoverServiceGrpc {

  private DiscoverServiceGrpc() {}

  public static final java.lang.String SERVICE_NAME = "sf.gateway.payment.v1.DiscoverService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<sf.gateway.payment.v1.Gateway.ServicesRequest,
      sf.gateway.payment.v1.Gateway.ServicesResponse> getServicesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Services",
      requestType = sf.gateway.payment.v1.Gateway.ServicesRequest.class,
      responseType = sf.gateway.payment.v1.Gateway.ServicesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<sf.gateway.payment.v1.Gateway.ServicesRequest,
      sf.gateway.payment.v1.Gateway.ServicesResponse> getServicesMethod() {
    io.grpc.MethodDescriptor<sf.gateway.payment.v1.Gateway.ServicesRequest, sf.gateway.payment.v1.Gateway.ServicesResponse> getServicesMethod;
    if ((getServicesMethod = DiscoverServiceGrpc.getServicesMethod) == null) {
      synchronized (DiscoverServiceGrpc.class) {
        if ((getServicesMethod = DiscoverServiceGrpc.getServicesMethod) == null) {
          DiscoverServiceGrpc.getServicesMethod = getServicesMethod =
              io.grpc.MethodDescriptor.<sf.gateway.payment.v1.Gateway.ServicesRequest, sf.gateway.payment.v1.Gateway.ServicesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Services"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  sf.gateway.payment.v1.Gateway.ServicesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  sf.gateway.payment.v1.Gateway.ServicesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new DiscoverServiceMethodDescriptorSupplier("Services"))
              .build();
        }
      }
    }
    return getServicesMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static DiscoverServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DiscoverServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DiscoverServiceStub>() {
        @java.lang.Override
        public DiscoverServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DiscoverServiceStub(channel, callOptions);
        }
      };
    return DiscoverServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static DiscoverServiceBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DiscoverServiceBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DiscoverServiceBlockingV2Stub>() {
        @java.lang.Override
        public DiscoverServiceBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DiscoverServiceBlockingV2Stub(channel, callOptions);
        }
      };
    return DiscoverServiceBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static DiscoverServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DiscoverServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DiscoverServiceBlockingStub>() {
        @java.lang.Override
        public DiscoverServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DiscoverServiceBlockingStub(channel, callOptions);
        }
      };
    return DiscoverServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static DiscoverServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<DiscoverServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<DiscoverServiceFutureStub>() {
        @java.lang.Override
        public DiscoverServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new DiscoverServiceFutureStub(channel, callOptions);
        }
      };
    return DiscoverServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void services(sf.gateway.payment.v1.Gateway.ServicesRequest request,
        io.grpc.stub.StreamObserver<sf.gateway.payment.v1.Gateway.ServicesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getServicesMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service DiscoverService.
   */
  public static abstract class DiscoverServiceImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return DiscoverServiceGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service DiscoverService.
   */
  public static final class DiscoverServiceStub
      extends io.grpc.stub.AbstractAsyncStub<DiscoverServiceStub> {
    private DiscoverServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DiscoverServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DiscoverServiceStub(channel, callOptions);
    }

    /**
     */
    public void services(sf.gateway.payment.v1.Gateway.ServicesRequest request,
        io.grpc.stub.StreamObserver<sf.gateway.payment.v1.Gateway.ServicesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getServicesMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service DiscoverService.
   */
  public static final class DiscoverServiceBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<DiscoverServiceBlockingV2Stub> {
    private DiscoverServiceBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DiscoverServiceBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DiscoverServiceBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public sf.gateway.payment.v1.Gateway.ServicesResponse services(sf.gateway.payment.v1.Gateway.ServicesRequest request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getServicesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service DiscoverService.
   */
  public static final class DiscoverServiceBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<DiscoverServiceBlockingStub> {
    private DiscoverServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DiscoverServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DiscoverServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public sf.gateway.payment.v1.Gateway.ServicesResponse services(sf.gateway.payment.v1.Gateway.ServicesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getServicesMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service DiscoverService.
   */
  public static final class DiscoverServiceFutureStub
      extends io.grpc.stub.AbstractFutureStub<DiscoverServiceFutureStub> {
    private DiscoverServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected DiscoverServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new DiscoverServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<sf.gateway.payment.v1.Gateway.ServicesResponse> services(
        sf.gateway.payment.v1.Gateway.ServicesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getServicesMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SERVICES = 0;

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
        case METHODID_SERVICES:
          serviceImpl.services((sf.gateway.payment.v1.Gateway.ServicesRequest) request,
              (io.grpc.stub.StreamObserver<sf.gateway.payment.v1.Gateway.ServicesResponse>) responseObserver);
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
          getServicesMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              sf.gateway.payment.v1.Gateway.ServicesRequest,
              sf.gateway.payment.v1.Gateway.ServicesResponse>(
                service, METHODID_SERVICES)))
        .build();
  }

  private static abstract class DiscoverServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    DiscoverServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return sf.gateway.payment.v1.Gateway.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("DiscoverService");
    }
  }

  private static final class DiscoverServiceFileDescriptorSupplier
      extends DiscoverServiceBaseDescriptorSupplier {
    DiscoverServiceFileDescriptorSupplier() {}
  }

  private static final class DiscoverServiceMethodDescriptorSupplier
      extends DiscoverServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    DiscoverServiceMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (DiscoverServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new DiscoverServiceFileDescriptorSupplier())
              .addMethod(getServicesMethod())
              .build();
        }
      }
    }
    return result;
  }
}
