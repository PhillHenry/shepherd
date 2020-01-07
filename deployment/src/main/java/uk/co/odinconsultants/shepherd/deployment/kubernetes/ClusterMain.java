package uk.co.odinconsultants.shepherd.deployment.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * see https://github.com/big-data-europe/docker-spark
 * see https://github.com/fabric8io/kubernetes-client/blob/master/kubernetes-examples/src/main/java/io/fabric8/kubernetes/examples/FullExample.java
 */
public class ClusterMain {

    private static final Logger logger = LoggerFactory.getLogger(ClusterMain.class);

    private static KubernetesClient client(String[] args) {
        KubernetesClient client = null;
        if (args.length == 1) {
            String master = args[0];
            Config config = new ConfigBuilder().withMasterUrl(master).build();
            client = new DefaultKubernetesClient(config);
        } else {
            client = new DefaultKubernetesClient();
        }
        return client;
    }

    private static final Watcher<ReplicationController> watcher = new Watcher<ReplicationController>() {
        @Override
        public void eventReceived(Action action, ReplicationController resource) {
            logger.info("{}: {}", action, resource);
        }

        @Override
        public void onClose(KubernetesClientException e) {
            if (e != null) {
                e.printStackTrace();
                logger.error(e.getMessage(), e);
            }
        }
    };

    public static void main(String[] args) throws InterruptedException {
        try (final KubernetesClient client = client(args)) {
            String namespace = "sparkshepherd";

            try (Watch watch = client.replicationControllers().inNamespace(namespace).withResourceVersion("0").watch(watcher)) {
                // Create a namespace for all our stuff
                Namespace ns = new NamespaceBuilder().withNewMetadata().withName(namespace).addToLabels("this", "rocks").endMetadata().build();
                log("Created namespace", client.namespaces().create(ns));

                // Get the namespace by name
                log("Get namespace by name", client.namespaces().withName(namespace).get());
                // Get the namespace by label
                log("Get namespace by label", client.namespaces().withLabel("this", "rocks").list());

                ResourceQuota quota = new ResourceQuotaBuilder().withNewMetadata().withName("pod-quota").endMetadata().withNewSpec().addToHard("pods", new Quantity("2")).endSpec().build();
                log("Create resource quota", client.resourceQuotas().inNamespace(namespace).create(quota));

                try {
                    log("Get jobs in namespace", client.batch().jobs().inNamespace(namespace).list());
                } catch (APIGroupNotAvailableException e) {
                    log("Skipping jobs example - extensions API group not available");
                }

                // Create an RC
                ReplicationController rc = new ReplicationControllerBuilder()
                        .withNewMetadata().withName("nginx-controller").addToLabels("server", "nginx").endMetadata()
                        .withNewSpec().withReplicas(1)
                        .withNewTemplate()
                        .withNewMetadata().addToLabels("server", "nginx").endMetadata()
                        .withNewSpec()
                        .addNewContainer().withName("nginx").withImage("nginx")
                        .addNewPort().withContainerPort(80).endPort()
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .endSpec().build();

                log("Created RC", client.replicationControllers().inNamespace(namespace).create(rc));

                log("Created RC with inline DSL",
                        client.replicationControllers().inNamespace(namespace).createNew()
                                .withNewMetadata().withName("nginx2-controller").addToLabels("server", "nginx").endMetadata()
                                .withNewSpec().withReplicas(0)
                                .withNewTemplate()
                                .withNewMetadata().addToLabels("server", "nginx2").endMetadata()
                                .withNewSpec()
                                .addNewContainer().withName("nginx").withImage("nginx")
                                .addNewPort().withContainerPort(80).endPort()
                                .endContainer()
                                .endSpec()
                                .endTemplate()
                                .endSpec().done());

                // Get the RC by name in namespace
                ReplicationController gotRc = client.replicationControllers().inNamespace(namespace).withName("nginx-controller").get();
                log("Get RC by name in namespace", gotRc);
                // Dump the RC as YAML
//                log("Dump RC as YAML", SerializationUtils.dumpAsYaml(gotRc));
//                log("Dump RC as YAML without state", SerializationUtils.dumpWithoutRuntimeStateAsYaml(gotRc));

                // Get the RC by label
                log("Get RC by label", client.replicationControllers().withLabel("server", "nginx").list());
                // Get the RC without label
                log("Get RC without label", client.replicationControllers().withoutLabel("server", "apache").list());
                // Get the RC with label in
                log("Get RC with label in", client.replicationControllers().withLabelIn("server", "nginx").list());
                // Get the RC with label in
                log("Get RC with label not in", client.replicationControllers().withLabelNotIn("server", "apache").list());
                // Get the RC by label in namespace
                log("Get RC by label in namespace", client.replicationControllers().inNamespace(namespace).withLabel("server", "nginx").list());
                // Update the RC
                client.replicationControllers().inNamespace(namespace).withName("nginx-controller").cascading(false).edit().editMetadata().addToLabels("new", "label").endMetadata().done();

                client.replicationControllers().inNamespace(namespace).withName("nginx-controller").scale(1);

                Thread.sleep(1000);

                // Update the RC - change the image to apache
                client.replicationControllers().inNamespace(namespace).withName("nginx-controller").edit().editSpec().editTemplate().withNewSpec()
                        .addNewContainer().withName("nginx").withImage("httpd")
                        .addNewPort().withContainerPort(80).endPort()
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .endSpec().done();

                Thread.sleep(1000);

                // Update the RC - change the image back to nginx using a rolling update
                // "Existing replica set doesn't exist"
//                client.replicationControllers().inNamespace("thisisatest").withName("nginx-controller").rolling().updateImage("nginx");

                Thread.sleep(1000);

                // Update the RC via rolling update with inline builder
                // java.lang.NoSuchMethodException: io.fabric8.kubernetes.api.model.apps.DoneableReplicaSet.<init>(io.fabric8.kubernetes.api.model.apps.ReplicaSet, io.fabric8.kubernetes.api.builder.Visitor)
//                client.replicationControllers().inNamespace("thisisatest").withName("nginx-controller")
//                        .rolling().edit().editMetadata().addToLabels("testing", "rolling-update").endMetadata().done();

                Thread.sleep(1000);

                //Update the RC inline
                client.replicationControllers().inNamespace(namespace).withName("nginx-controller").edit()
                        .editMetadata()
                        .addToLabels("another", "label")
                        .endMetadata()
                        .done();

                log("Updated RC");
                // Clean up the RC
                client.replicationControllers().inNamespace(namespace).withName("nginx-controller").delete();
                client.replicationControllers().inNamespace(namespace).withName("nginx2-controller").delete();
                log("Deleted RCs");

                //Create another RC inline
                client.replicationControllers().inNamespace(namespace).createNew().withNewMetadata().withName("nginx-controller").addToLabels("server", "nginx").endMetadata()
                        .withNewSpec().withReplicas(3)
                        .withNewTemplate()
                        .withNewMetadata().addToLabels("server", "nginx").endMetadata()
                        .withNewSpec()
                        .addNewContainer().withName("nginx").withImage("nginx")
                        .addNewPort().withContainerPort(80).endPort()
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .endSpec().done();
                log("Created inline RC");

                Thread.sleep(1000);

                client.replicationControllers().inNamespace(namespace).withName("nginx-controller").delete();
                log("Deleted RC");

                log("Created RC", client.replicationControllers().inNamespace(namespace).create(rc));
                client.replicationControllers().inAnyNamespace().withLabel("server", "nginx").delete();
                log("Deleted RC by label");

                log("Created RC", client.replicationControllers().inNamespace(namespace).create(rc));
                client.replicationControllers().inNamespace(namespace).withField("metadata.name", "nginx-controller").delete();
                log("Deleted RC by field");

                log("Created service",
                        client.services().inNamespace(namespace).createNew()
                                .withNewMetadata().withName("testservice").endMetadata()
                                .withNewSpec()
                                .addNewPort().withPort(80).withNewTargetPort().withIntVal(80).endTargetPort().endPort()
                                .endSpec()
                                .done());
                log("Updated service", client.services().inNamespace(namespace).withName("testservice").edit().editMetadata().addToLabels("test", "label").endMetadata().done());
                client.replicationControllers().inNamespace(namespace).withField("metadata.name", "testservice").delete();
                log("Deleted service by field");

                log("Root paths:", client.rootPaths());

            } finally {
                // And finally clean up the namespace
                client.namespaces().withName(namespace).delete();
                log("Deleted namespace");
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage(), e);


            Throwable[] suppressed = e.getSuppressed();
            if (suppressed != null) {
                for (Throwable t : suppressed) {
                    logger.error(t.getMessage(), t);
                }
            }
        }
    }

    private static void log(String action, Object obj) {
        logger.info("{}: {}", action, obj);
    }

    private static void log(String action) {
        logger.info(action);
    }

}
