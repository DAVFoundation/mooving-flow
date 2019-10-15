package org.dav.vehicle_rider.token_payment;

import java.math.BigDecimal;
import java.util.Arrays;

import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.cassandra.CassandraIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.flink.api.java.utils.ParameterTool;
import org.dav.Json;
import org.dav.config.Config;
import org.dav.vehicle_rider.cassandra.FailedTxn;
import org.dav.vehicle_rider.cassandra.PendingTxn;
import org.dav.vehicle_rider.cassandra_helpers.UpdateOwnerDavBalanceBase;
import org.dav.vehicle_rider.cassandra_helpers.UpdateOwnerDavBalanceDelta;
import org.dav.vehicle_rider.messages.DavBalanceDeltaUpdateMessage;
import org.joda.time.Duration;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.web3j.crypto.Credentials;

public class TokenPaymentFlow {

    private static final Logger log = LoggerFactory.getLogger(TokenPaymentFlow.class);
    private static final String CASSANDRA_SCEHME_NAME = "vehicle_rider";

    public static void main(String[] args) {
        Config config = Config.create(true, args);
        Pipeline pipeline = Pipeline.create(config.pipelineOptions());

        ParameterTool parameters = ParameterTool.fromArgs(args);
        String contractAddress = parameters.get("contract_address");
        String ethNodeUrl = parameters.get("eth_node_url");
        String riderPrivateKey = parameters.get("rider_private_key");
        String networkOperatorPrivateKey = parameters.get("network_operator_private_key");

        Credentials riderCredentials = Credentials.create(riderPrivateKey);
        Credentials networkOperatorCredentials = Credentials.create(networkOperatorPrivateKey);

        int fixedWindowTime = 60 * 60 * 4; // 4 hours

        TupleTag<TokenPaymentMessage> successfulTxnTag = new TupleTag<TokenPaymentMessage>() {

            private static final long serialVersionUID = 1L;
        };
        TupleTag<TokenPaymentMessage> failedTxnTag = new TupleTag<TokenPaymentMessage>() {

            private static final long serialVersionUID = 1L;
        };

        PCollection<TokenPaymentMessage> tokenPaymentMessages = pipeline
                .apply("ReadLines",
                        KafkaIO.readBytes().withBootstrapServers(config.kafkaSeed() + ":" + config.kafkaPort())
                                .withTopic("token-payment")
                                .updateConsumerProperties(ImmutableMap.of("group.id", "token-payment"))
                                .commitOffsetsInFinalize().withoutMetadata())

                .apply(MapElements.into(TypeDescriptors.strings()).via(t -> {
                    return new String(t.getValue());
                })).apply(MapElements.into(TypeDescriptor.of(TokenPaymentMessage.class)).via(message -> {
                    try {
                        return Json.parse(message, TokenPaymentMessage.class, false);
                    } catch (Exception e) {
                        log.info("Error deserializing token-payment message: " + e.toString());
                        throw e;
                    }
                })).apply(Window.<TokenPaymentMessage>into(FixedWindows.of(Duration.standardSeconds(fixedWindowTime))));
        PCollection<TokenPaymentMessage> NetworkOperatorToRiderUnifiedTxn = tokenPaymentMessages
                .apply("Filter only network operator to rider payments", Filter.by(
                        tokenPayment -> tokenPayment.getTransactionType() == TransactionType.NetworkOperatorToRider))
                .apply("Map operator to rider txns to kv",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                                        TypeDescriptor.of(TokenPaymentMessage.class)))
                                .via(tokenPayment -> KV.of("OperatorToRiderTxn", tokenPayment)))
                .apply("Group all operator to rider txns", GroupByKey.<String, TokenPaymentMessage>create())
                .apply("Create one txns for all operator to rider txns",
                        ParDo.of(new DoFn<KV<String, Iterable<TokenPaymentMessage>>, TokenPaymentMessage>() {
                            private static final long serialVersionUID = 1L;

                            @ProcessElement
                            public void ProcessElement(ProcessContext c) {
                                Iterable<TokenPaymentMessage> operatorToRiderTxns = c.element().getValue();
                                BigDecimal totalTxnAmount = BigDecimal.ZERO;
                                for (TokenPaymentMessage message : operatorToRiderTxns) {
                                    totalTxnAmount = totalTxnAmount.add(message.getAmount());
                                }
                                c.output(new TokenPaymentMessage(TransactionType.NetworkOperatorToRider,
                                        totalTxnAmount));
                            }
                        }));

        PCollection<TokenPaymentMessage> ownerInvolvedPayments = tokenPaymentMessages
                .apply("Filter only owner involved payments",
                        Filter.by(tokenPayment -> tokenPayment.getTransactionType() == TransactionType.OwnerToRider
                                || tokenPayment.getTransactionType() == TransactionType.RiderToOwner))
                .apply("Create owner to transaction KV",
                        MapElements
                                .into(TypeDescriptors.kvs(TypeDescriptors.strings(),
                                        TypeDescriptor.of(TokenPaymentMessage.class)))
                                .via(tokenPayment -> KV.of(tokenPayment.getOwnerId().toString(), tokenPayment)))
                .apply("Group token payments by owner id", GroupByKey.<String, TokenPaymentMessage>create())
                .apply("aggregate payments", ParDo.of(new AggregateOwnerInvolvedTransactions()));

        PCollection<TokenPaymentMessage> messagesWithBlockChainData = PCollectionList
                .of(NetworkOperatorToRiderUnifiedTxn).and(ownerInvolvedPayments)
                .apply(Flatten.<TokenPaymentMessage>pCollections())
                .apply("Add blockchain data to messages", ParDo.of(new AddBlockchainData(config.cassandraSeed(),
                        riderCredentials.getAddress(), networkOperatorCredentials.getAddress(), log)));

        PCollection<TokenPaymentMessage> messagesWaitingForPendingTx = messagesWithBlockChainData
                .apply("create blockchain txns",
                        ParDo.of(new WaitWhilePendingTx(config.cassandraSeed(), riderPrivateKey, networkOperatorPrivateKey, log)));

        messagesWaitingForPendingTx.apply("Create pending txns", ParDo.of(new CreatePendingTxnRecord(riderPrivateKey, networkOperatorPrivateKey, log)))
                .apply("write pending txns to cassandra",
                CassandraIO.<PendingTxn>write().withHosts(Arrays.asList(config.cassandraSeed()))
                        .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                        .withEntity(PendingTxn.class));

        PCollectionTuple messagesAfterSendingToBlockchain = messagesWaitingForPendingTx.apply("create blockchain txns",
                ParDo.of(new CreateTokenTx(riderPrivateKey, networkOperatorPrivateKey, contractAddress, ethNodeUrl,
                        successfulTxnTag, failedTxnTag, log))
                        .withOutputTags(successfulTxnTag, TupleTagList.of(failedTxnTag)));

        PCollection<TokenPaymentMessage> successfulTxns = messagesAfterSendingToBlockchain.get(successfulTxnTag);
        PCollection<TokenPaymentMessage> failedTxns = messagesAfterSendingToBlockchain.get(failedTxnTag);

        successfulTxns.apply("Remove pending txn", ParDo.of(new RemovePendingTxn(config.cassandraSeed(), riderPrivateKey, networkOperatorPrivateKey, log)));

        PCollection<TokenPaymentMessage> ownerRelatedSuccessfulTxns = successfulTxns
                .apply("Filter owner related txns",
                        Filter.by(tokenPayment -> tokenPayment.transactionType == TransactionType.OwnerToRider
                                || tokenPayment.transactionType == TransactionType.RiderToOwner));

        ownerRelatedSuccessfulTxns
                .apply("Create dav balance delta update message",
                        MapElements.into(TypeDescriptor.of(DavBalanceDeltaUpdateMessage.class)).via(tokenPayment -> {
                            // in next phase we add txn amount to owner balance delta, so if it's rider to owner
                            // txn, we need to add negative value
                            BigDecimal amountFactor = BigDecimal.valueOf(1);
                            if (tokenPayment.transactionType == TransactionType.RiderToOwner) {
                                amountFactor = BigDecimal.valueOf(-1);
                            }
                            return new DavBalanceDeltaUpdateMessage(tokenPayment.getOwnerId(),
                                    tokenPayment.amount.multiply(amountFactor));
                        }))
                .apply("Update owner dav balance delta", ParDo
                        .of(new UpdateOwnerDavBalanceDelta<DavBalanceDeltaUpdateMessage>(config.cassandraSeed(), log)));

        ownerRelatedSuccessfulTxns
                .apply("Create dav balance base update message",
                        ParDo.of(new CreateUpdateBaseBalanceMessage( contractAddress, ethNodeUrl, log)))
                .apply("Update owner dav balance base", ParDo
                        .of(new UpdateOwnerDavBalanceBase<DavBalanceBaseUpdateMessage>(config.cassandraSeed(), log)));

        failedTxns.apply("Create failed txns", ParDo.of(new CreateFailedTxnRecord(riderPrivateKey, networkOperatorPrivateKey, log)))
                .apply("write failed txns to cassandra",
                CassandraIO.<FailedTxn>write().withHosts(Arrays.asList(config.cassandraSeed()))
                        .withPort(config.cassandraPort()).withKeyspace(CASSANDRA_SCEHME_NAME)
                        .withEntity(FailedTxn.class));

        pipeline.run();
    }
}
