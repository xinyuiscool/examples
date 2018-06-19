package com.linkedin.beam.examples;

import com.linkedin.beam.examples.config.ConfigForExamples;
import com.linkedin.beam.values.PCollectionRWTable;
import com.linkedin.beam.values.ROTable;
import com.linkedin.beam.values.PCollectionROTable;
import com.linkedin.beam.values.RWTable;
import com.linkedin.beam.values.TableContext;
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
    final SamzaPipelineOptions pipelineOpts = ConfigForExamples.getSamzaPipelineOptions("identity-example");
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
    final PCollectionRWTable<KV<String, InternalSetting>> settingsTable =
        internalSettings.apply(
            RocksDbTable.readWrite()
                .withName("internalSettings")
                .withKeyCoder(StringUtf8Coder.of())
                .withValueCoder(AvroCoder.of(PageViewEvent.class)));

    // Remote Table
    final PCollectionROTable<KV<String, Profile>> profileTable =
        pipeline.apply(
            EspressoTable.readOnly()
                .withDb("isb")
                .withTable("profile"));

    // A pipeline consumes Kafka PageViewEvent and look up the internal settings
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

                RWTable<String, InternalSetting> settings = tc.getTable(settingsTable);
                ROTable<String, Profile> profile = tc.getTable(profileTable);

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
