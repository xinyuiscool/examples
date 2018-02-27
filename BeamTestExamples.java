public class BeamTestExamples {

  //this example uses in-memory data structure
  public void testList() {
    final SamzaPipelineOptions pipelineOpts = ConfigForExamples.getSamzaPipelineOptions("assert-example");
    final Pipeline pipeline = Pipeline.create(pipelineOpts);

    final PCollection<KV<String, Integer>> input = pipeline
        .apply(Create.of(Arrays.asList(
            KV.of("a", 1),
            KV.of("a", 1),
            KV.of("a", 4),
            KV.of("b", 1),
            KV.of("b", 13)
        )));

    PCollection<Integer> max = input.apply(Values.<Integer>create()).apply(Max.globally());
    PCollection<KV<String, Double>> meanPerKey = input.apply(Mean.perKey());

    PAssert.that(max).containsInAnyOrder(13);
    PAssert.that(meanPerKey).containsInAnyOrder(Arrays.asList(KV.of("a", 2.0), KV.of("b", 7.0)));

    pipeline.run().waitUntilFinish();    
  }
  
  //this example uses file
  public void testFile() {
    final SamzaPipelineOptions pipelineOpts = ConfigForExamples.getSamzaPipelineOptions("word-count-example");
    final Pipeline pipeline = Pipeline.create(pipelineOpts);

    PCollection<Integer> count = pipeline
        .apply(TextIO.read().from("java-examples/test_data/words.txt"))
        .apply("Extract Words", FlatMapElements
            .into(TypeDescriptors.strings())
            .via((String line) -> Arrays.asList(line.split("[^a-zA-Z']+"))))
        .apply("Filter empty words", Filter.by(word -> !word.isEmpty()))
        .apply(Count.globally());
    
    // total is 1000 words
    PAssert.that(count).containsInAnyOrder(1000);
  }
  
  //this example uses stream builder
  public void testStream() {
    TestStream<Long> source = TestStream.create(VarLongCoder.of())
        .addElements(TimestampedValue.of(1L, new Instant(1000L)),
            TimestampedValue.of(2L, new Instant(2000L)))
        .advanceProcessingTime(Duration.standardMinutes(12))
        .addElements(TimestampedValue.of(3L, new Instant(3000L)))
        .advanceProcessingTime(Duration.standardMinutes(6))
        .advanceWatermarkToInfinity();

    PCollection<Long> sum = p.apply(source)
        .apply(Window.<Long>configure().triggering(AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(AfterProcessingTime.pastFirstElementInPane()
                .plusDelayOf(Duration.standardMinutes(5)))).accumulatingFiredPanes()
            .withAllowedLateness(Duration.ZERO))
        .apply(Sum.longsGlobally());

    PAssert.that(sum).inEarlyGlobalWindowPanes().containsInAnyOrder(3L, 6L);

    p.run();
  }

}
