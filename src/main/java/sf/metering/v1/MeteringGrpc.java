package sf.metering.v1;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 */
@io.grpc.stub.annotations.GrpcGenerated
public final class MeteringGrpc {

  private MeteringGrpc() {}

  public static final java.lang.String SERVICE_NAME = "sf.metering.v1.Metering";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<sf.metering.v1.MeteringOuterClass.Events,
      com.google.protobuf.Empty> getEmitMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "Emit",
      requestType = sf.metering.v1.MeteringOuterClass.Events.class,
      responseType = com.google.protobuf.Empty.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<sf.metering.v1.MeteringOuterClass.Events,
      com.google.protobuf.Empty> getEmitMethod() {
    io.grpc.MethodDescriptor<sf.metering.v1.MeteringOuterClass.Events, com.google.protobuf.Empty> getEmitMethod;
    if ((getEmitMethod = MeteringGrpc.getEmitMethod) == null) {
      synchronized (MeteringGrpc.class) {
        if ((getEmitMethod = MeteringGrpc.getEmitMethod) == null) {
          MeteringGrpc.getEmitMethod = getEmitMethod =
              io.grpc.MethodDescriptor.<sf.metering.v1.MeteringOuterClass.Events, com.google.protobuf.Empty>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "Emit"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  sf.metering.v1.MeteringOuterClass.Events.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  com.google.protobuf.Empty.getDefaultInstance()))
              .setSchemaDescriptor(new MeteringMethodDescriptorSupplier("Emit"))
              .build();
        }
      }
    }
    return getEmitMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MeteringStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MeteringStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MeteringStub>() {
        @java.lang.Override
        public MeteringStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MeteringStub(channel, callOptions);
        }
      };
    return MeteringStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports all types of calls on the service
   */
  public static MeteringBlockingV2Stub newBlockingV2Stub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MeteringBlockingV2Stub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MeteringBlockingV2Stub>() {
        @java.lang.Override
        public MeteringBlockingV2Stub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MeteringBlockingV2Stub(channel, callOptions);
        }
      };
    return MeteringBlockingV2Stub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MeteringBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MeteringBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MeteringBlockingStub>() {
        @java.lang.Override
        public MeteringBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MeteringBlockingStub(channel, callOptions);
        }
      };
    return MeteringBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MeteringFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MeteringFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MeteringFutureStub>() {
        @java.lang.Override
        public MeteringFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MeteringFutureStub(channel, callOptions);
        }
      };
    return MeteringFutureStub.newStub(factory, channel);
  }

  /**
   */
  public interface AsyncService {

    /**
     */
    default void emit(sf.metering.v1.MeteringOuterClass.Events request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getEmitMethod(), responseObserver);
    }
  }

  /**
   * Base class for the server implementation of the service Metering.
   */
  public static abstract class MeteringImplBase
      implements io.grpc.BindableService, AsyncService {

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return MeteringGrpc.bindService(this);
    }
  }

  /**
   * A stub to allow clients to do asynchronous rpc calls to service Metering.
   */
  public static final class MeteringStub
      extends io.grpc.stub.AbstractAsyncStub<MeteringStub> {
    private MeteringStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MeteringStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MeteringStub(channel, callOptions);
    }

    /**
     */
    public void emit(sf.metering.v1.MeteringOuterClass.Events request,
        io.grpc.stub.StreamObserver<com.google.protobuf.Empty> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getEmitMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * A stub to allow clients to do synchronous rpc calls to service Metering.
   */
  public static final class MeteringBlockingV2Stub
      extends io.grpc.stub.AbstractBlockingStub<MeteringBlockingV2Stub> {
    private MeteringBlockingV2Stub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MeteringBlockingV2Stub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MeteringBlockingV2Stub(channel, callOptions);
    }

    /**
     */
    public com.google.protobuf.Empty emit(sf.metering.v1.MeteringOuterClass.Events request) throws io.grpc.StatusException {
      return io.grpc.stub.ClientCalls.blockingV2UnaryCall(
          getChannel(), getEmitMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do limited synchronous rpc calls to service Metering.
   */
  public static final class MeteringBlockingStub
      extends io.grpc.stub.AbstractBlockingStub<MeteringBlockingStub> {
    private MeteringBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MeteringBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MeteringBlockingStub(channel, callOptions);
    }

    /**
     */
    public com.google.protobuf.Empty emit(sf.metering.v1.MeteringOuterClass.Events request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getEmitMethod(), getCallOptions(), request);
    }
  }

  /**
   * A stub to allow clients to do ListenableFuture-style rpc calls to service Metering.
   */
  public static final class MeteringFutureStub
      extends io.grpc.stub.AbstractFutureStub<MeteringFutureStub> {
    private MeteringFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MeteringFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MeteringFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<com.google.protobuf.Empty> emit(
        sf.metering.v1.MeteringOuterClass.Events request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getEmitMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_EMIT = 0;

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
        case METHODID_EMIT:
          serviceImpl.emit((sf.metering.v1.MeteringOuterClass.Events) request,
              (io.grpc.stub.StreamObserver<com.google.protobuf.Empty>) responseObserver);
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
          getEmitMethod(),
          io.grpc.stub.ServerCalls.asyncUnaryCall(
            new MethodHandlers<
              sf.metering.v1.MeteringOuterClass.Events,
              com.google.protobuf.Empty>(
                service, METHODID_EMIT)))
        .build();
  }

  private static abstract class MeteringBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MeteringBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return sf.metering.v1.MeteringOuterClass.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Metering");
    }
  }

  private static final class MeteringFileDescriptorSupplier
      extends MeteringBaseDescriptorSupplier {
    MeteringFileDescriptorSupplier() {}
  }

  private static final class MeteringMethodDescriptorSupplier
      extends MeteringBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final java.lang.String methodName;

    MeteringMethodDescriptorSupplier(java.lang.String methodName) {
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
      synchronized (MeteringGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MeteringFileDescriptorSupplier())
              .addMethod(getEmitMethod())
              .build();
        }
      }
    }
    return result;
  }
}
