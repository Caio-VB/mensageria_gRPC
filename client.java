import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import messaging.*;

import java.util.concurrent.TimeUnit;

public class MessagingClient {
    private final ManagedChannel channel;
    private final MessagingServiceGrpc.MessagingServiceBlockingStub blockingStub;
    private final MessagingServiceGrpc.MessagingServiceStub asyncStub;

    public MessagingClient(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        this.blockingStub = MessagingServiceGrpc.newBlockingStub(channel);
        this.asyncStub = MessagingServiceGrpc.newStub(channel);
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void createChannel(String name, ChannelType type) {
        CreateChannelRequest request = CreateChannelRequest.newBuilder().setName(name).setType(type).build();
        CreateChannelResponse response = blockingStub.createChannel(request);
        System.out.println("Create Channel Response: " + response.getSuccess());
    }

    public void removeChannel(String name) {
        RemoveChannelRequest request = RemoveChannelRequest.newBuilder().setName(name).build();
        RemoveChannelResponse response = blockingStub.removeChannel(request);
        System.out.println("Remove Channel Response: " + response.getSuccess());
    }

    public void listChannels() {
        ListChannelsRequest request = ListChannelsRequest.newBuilder().build();
        ListChannelsResponse response = blockingStub.listChannels(request);
        response.getChannelsList().forEach(channel -> {
            System.out.println("Channel: " + channel.getName() + ", Type: " + channel.getType() + ", Pending Messages: " + channel.getPendingMessages());
        });
    }

    public void publishMessage(String channelName, byte[] message) {
        PublishMessageRequest request = PublishMessageRequest.newBuilder().setChannelName(channelName).setMessage(com.google.protobuf.ByteString.copyFrom(message)).build();
        PublishMessageResponse response = blockingStub.publishMessage(request);
        System.out.println("Publish Message Response: " + response.getSuccess());
    }

    public void subscribeChannel(String channelName) {
        SubscribeChannelRequest request = SubscribeChannelRequest.newBuilder().setChannelName(channelName).build();
        asyncStub.subscribeChannel(request, new StreamObserver<Message>() {
            @Override
            public void onNext(Message message) {
                System.out.println("Received Message: " + message.getMessage().toStringUtf8());
            }

            @Override
            public void onError(Throwable t) {
                System.err.println("Error: " + t.getMessage());
            }

            @Override
            public void onCompleted() {
                System.out.println("Subscription Completed");
            }
        });
    }

    public static void main(String[] args) throws InterruptedException {
        MessagingClient client = new MessagingClient("localhost", 9090);
        client.createChannel("testChannel", ChannelType.SIMPLE);
        client.listChannels();
        client.publishMessage("testChannel", "Hello, World!".getBytes());
        client.subscribeChannel("testChannel");
        Thread.sleep(5000); // Wait to receive messages
        client.shutdown();
    }
}
