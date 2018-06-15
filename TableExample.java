package com.linkedin.beam.examples;

import com.linkedin.beam.examples.config.ConfigForExamples;
import com.linkedin.beam.examples.kvtable.KVTable;
import com.linkedin.beam.examples.kvtable.PCollectionTable;
import com.linkedin.beam.examples.kvtable.TableContext;
import com.linkedin.beam.io.BrooklinIO;
import com.linkedin.beam.io.BrooklinIOConfig;
import com.linkedin.beam.io.LiKafkaIO;
import com.linkedin.beam.io.LiKafkaIOConfig;
import com.linkedin.events.PageViewEvent;
import com.linkedin.identity.Profile;
import com.linkedin.identity.internal.InternalSetting;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import static com.linkedin.beam.io.BrooklinIO.ESPRESSO;
import static com.linkedin.beam.io.LiKafkaIOConfig.ClusterName.TRACKING;

public class TableExample {

  public static void main(String[] args) {
    final SamzaPipelineOptions pipelineOpts = ConfigForExamples.getSamzaPipelineOptions("table-example");
    final LiKafkaIOConfig kafkaConfig = ConfigForExamples.getLiKafkaIOConfig();
    final BrooklinIOConfig brooklinConfig = ConfigForExamples.getBrooklinIOConfig();
    final Pipeline pipeline = Pipeline.create(pipelineOpts);

    // A Brooklin stream
    final PCollection<KV<String, InternalSetting>> internalSettings = pipeline
        .apply(BrooklinIO.<GenericRecord, InternalSetting>read()
            .withConnectorName(ESPRESSO)
            .withStream("beam-test-brooklin-internal-setting")
            .withTimestampFn(kv -> new Instant(kv.getValue().getLastModified()))
            .withConfig(brooklinConfig)
            .withoutMetadata())
        .apply(Values.create())
        .apply(WithKeys
            .of(setting -> setting.getMemberId().toString()));

    //Local Table
    final PCollectionTable<KV<String, InternalSetting>> settingsTable =
        internalSettings.apply(
            RocksDbTable.write()
                .withName("internalSettings")
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(AvroCoder.of(PageViewEvent.class)));

    // Remote Table
    final PCollectionTable<KV<String, Profile>> profileTable =
        pipeline.apply(
            EspressoTable.read()
                .withDb("isb")
                .withTable("profile"));

    // A pipeline consumes Kafka PageViewEvent and look up the both tables
    pipeline
        .apply(LiKafkaIO.<PageViewEvent>read()
            .withTopic("PageViewEvent")
            .withConsumerConfig(kafkaConfig.getConsumerConfig(TRACKING))
            .withoutMetadata())
        .apply(ParDo.of(
            new DoFn<KV<String, PageViewEvent>, Void>() {

              @ProcessElement
              public void processElement(ProcessContext c,
                                         @TableContext.Inject TableContext tc) {

                KVTable<String, InternalSetting> settings = tc.getTable(settingsTable);
                KVTable<String, Profile> profile = tc.getTable(profileTable);

                PageViewEvent pv = c.element().getValue();
                String memberId = String.valueOf(pv.header.memberId);
                //table lookup
                InternalSetting is = settings.get(memberId);
                Profile p = profile.get(memberId);
                // do something ...
              }
        }));
  }
}
