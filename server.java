import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import messaging.*;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MessagingServer {
    private final Server server;
    private final ConcurrentHashMap<String, Channel> channels;

    public MessagingServer(int port) {
        this.server = ServerBuilder.forPort(port)
            .addService(new MessagingServiceImpl())
            .build();
        this.channels = new ConcurrentHashMap<>();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("Server started, listening on " + server.getPort());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("* Shutting down gRPC server since JVM is shutting down");
            this.stop();
            System.err.println("* Server shut down");
        }));
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private class MessagingServiceImpl extends MessagingServiceGrpc.MessagingServiceImplBase {
        @Override
        public void createChannel(CreateChannelRequest request, StreamObserver<CreateChannelResponse> responseObserver) {
            String name = request.getName();
            ChannelType type = request.getType();
            if (channels.putIfAbsent(name, new Channel(name, type)) == null) {
                responseObserver.onNext(CreateChannelResponse.newBuilder().setSuccess(true).build());
            } else {
                responseObserver.onNext(CreateChannelResponse.newBuilder().setSuccess(false).build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void removeChannel(RemoveChannelRequest request, StreamObserver<RemoveChannelResponse> responseObserver) {
            String name = request.getName();
            if (channels.remove(name) != null) {
                responseObserver.onNext(RemoveChannelResponse.newBuilder().setSuccess(true).build());
            } else {
                responseObserver.onNext(RemoveChannelResponse.newBuilder().setSuccess(false).build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void listChannels(ListChannelsRequest request, StreamObserver<ListChannelsResponse> responseObserver) {
            ListChannelsResponse.Builder responseBuilder = ListChannelsResponse.newBuilder();
            channels.forEach((name, channel) -> {
                responseBuilder.addChannels(ChannelInfo.newBuilder()
                    .setName(name)
                    .setType(channel.getType())
                    .setPendingMessages(channel.getPendingMessagesCount())
                    .build());
            });
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }

        @Override
        public void publishMessage(PublishMessageRequest request, StreamObserver<PublishMessageResponse> responseObserver) {
            String channelName = request.getChannelName();
            byte[] message = request.getMessage().toByteArray();
            Channel channel = channels.get(channelName);
            if (channel != null) {
                channel.publishMessage(message);
                responseObserver.onNext(PublishMessageResponse.newBuilder().setSuccess(true).build());
            } else {
                responseObserver.onNext(PublishMessageResponse.newBuilder().setSuccess(false).build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void subscribeChannel(SubscribeChannelRequest request, StreamObserver<Message> responseObserver) {
            String channelName = request.getChannelName();
            Channel channel = channels.get(channelName);
            if (channel != null) {
                channel.subscribe(responseObserver);
            } else {
                responseObserver.onError(new RuntimeException("Channel not found"));
            }
        }

        @Override
        public void getMessage(GetMessageRequest request, StreamObserver<GetMessageResponse> responseObserver) {
            String channelName = request.getChannelName();
            Channel channel = channels.get(channelName);
            if (channel != null) {
                byte[] message = channel.getMessage(request.getTimeout());
                if (message != null) {
                    responseObserver.onNext(GetMessageResponse.newBuilder().setMessage(com.google.protobuf.ByteString.copyFrom(message)).build());
                } else {
                    responseObserver.onNext(GetMessageResponse.newBuilder().build());
                }
            } else {
                responseObserver.onError(new RuntimeException("Channel not found"));
            }
            responseObserver.onCompleted();
        }
    }

    private class Channel {
        private final String name;
        private final ChannelType type;
        private final ConcurrentLinkedQueue<byte[]> messages;
        private final ConcurrentLinkedQueue<StreamObserver<Message>> subscribers;

        public Channel(String name, ChannelType type) {
            this.name = name;
            this.type = type;
            this.messages = new ConcurrentLinkedQueue<>();
            this.subscribers = new ConcurrentLinkedQueue<>();
        }

        public ChannelType getType() {
            return type;
        }

        public int getPendingMessagesCount() {
            return messages.size();
        }

        public void publishMessage(byte[] message) {
            messages.add(message);
            if (type == ChannelType.MULTIPLE) {
                for (StreamObserver<Message> subscriber : subscribers) {
                    subscriber.onNext(Message.newBuilder().setMessage(com.google.protobuf.ByteString.copyFrom(message)).build());
                }
            } else if (!subscribers.isEmpty()) {
                StreamObserver<Message> subscriber = subscribers.poll();
                subscriber.onNext(Message.newBuilder().setMessage(com.google.protobuf.ByteString.copyFrom(message)).build());
            }
        }

        public void subscribe(StreamObserver<Message> subscriber) {
            subscribers.add(subscriber);
        }

        public byte[] getMessage(int timeout) {
            return messages.poll();
        }
    }

    public static void main(String[] args) throws IOException {
        MessagingServer server = new MessagingServer(9090);
        server.start();
    }
}
