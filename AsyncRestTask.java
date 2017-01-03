public class AsyncRestTask implements AsyncStreamTask, InitableTask, ClosableTask {
  private Client client;
  private WebTarget target;

  @Override
  public void init(Config config, TaskContext taskContext) throws Exception {
    client = ClientBuilder.newClient();
    target = client.target("http://example.com/resource/").path("hello");
  }

  @Override
  public void processAsync(IncomingMessageEnvelope envelope, MessageCollector collector,
      TaskCoordinator coordinator, final TaskCallback callback) {
    target.request().async().get(new InvocationCallback<Response>() {
      @Override
      public void completed(Response response) {
        System.out.println("Response status code " + response.getStatus() + " received.");
        callback.complete();
      }

      @Override
      public void failed(Throwable throwable) {
        System.out.println("Invocation failed.");
        callback.failure(throwable);
      }
    });
  }

  @Override
  public void close() throws Exception {
    client.close();
  }
}
