package com.linkedin.beam.store;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** 
 * This class does look up of remote store using the key of main input PColllection.
 * The result PCollection will be KV<Key, <input value, store value>>
 */
public class StoreLookup<KS, VS, VT>
    extends PTransform<PCollection<KV<KS, VS>>, PCollection<KV<KS, KV<VS, VT>>>> {

  public static <KS, VS, VT> StoreLookup<KS, VS, VT> of(Map<String, String> storeConfig) {
    return new StoreLookup<>(storeConfig);
  }

  private final Map<String, String> storeConfig;
  private final Coder<VT> storeValueCoder;

  private StoreLookup(Map<String, String> storeConfig) {
    this.storeConfig = storeConfig;
    // Need to set the store value coder for real here.
    this.storeValueCoder = null;
  }

  @Override
  public PCollection<KV<KS, KV<VS, VT>>> expand(PCollection<KV<KS, VS>> input) {
    final KvCoder inputKVCoder = (KvCoder) input.getCoder();
    final Coder<KS> keyCoder = inputKVCoder.getKeyCoder();
    final Coder<VS> valueCoder = inputKVCoder.getValueCoder();

    return input
        .apply(ParDo.of(
            new DoFn<KV<KS, VS>, KV<KS, KV<VS, VT>>>() {
              private transient StoreClient<KS, VT> storeClient;

              @Setup
              public void setup() {
                storeClient = createStoreClient(storeConfig);
              }

              @ProcessElement
              public void process(ProcessContext context) {
                KS key = context.element().getKey();
                VT val = storeClient.get(key);
                context.output(KV.of(key, KV.of(context.element().getValue(), val)));
              }
          }))
        .setCoder(KvCoder.of(keyCoder, KvCoder.of(valueCoder, storeValueCoder)));
  }


  interface StoreClient<K, V> {
    V get(K key);
  }

  static StoreClient createStoreClient(Map<String, String> config) {
    return null;
  }

  /**
   * Let's use the above code to do remote store lookup
   */
  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    Pipeline pipeline = Pipeline.create(options);

    // dummy main input
    PCollection<KV<Integer, String>> input = pipeline.apply(Create.of(KV.of(1, "a")));
    // store config
    Map<String, String> storeConfig = ImmutableMap.of("connect.url", "127.0.0.1");
    // look up store based on the input key
    PCollection<KV<Integer, KV<String, String>>> output = input.apply(StoreLookup.of(storeConfig));
    
    pipeline.run().waitUntilFinish();
  }
}
