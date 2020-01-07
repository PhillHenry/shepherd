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
        final String namespace = "sparkshepherd";
        final String image = "bde2020/spark-master:2.4.4-hadoop2.7";
        final String controller = "spark-controller";
        try (final KubernetesClient client = client(args)) {

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

                // Create master

                EnvVar initDaemonStep = new EnvVar();
                initDaemonStep.setName("INIT_DAEMON_STEP");
                initDaemonStep.setValue("setup_spark");

                ReplicationController rc = new ReplicationControllerBuilder()
                        .withNewMetadata().withName(controller).addToLabels("server", "master").endMetadata()
                        .withNewSpec().withReplicas(1)
                        .withNewTemplate()
                        .withNewMetadata().addToLabels("server", "master").endMetadata()
                        .withNewSpec()
                        .addNewContainer().withName("master").withImage(image)
                        .addNewPort().withContainerPort(8080).withHostPort(8080).withHostIP("127.0.0.1").endPort()
                        .addNewPort().withContainerPort(7077).withHostPort(7077).endPort()
                        .addToEnv(initDaemonStep)
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .endSpec().build();

                log("Created RC", client.replicationControllers().inNamespace(namespace).create(rc));

                client.replicationControllers().inNamespace(namespace).withName(controller).scale(1);

                Thread.sleep(1000);

                // Clean up the RC
//                client.replicationControllers().inNamespace(namespace).withName(controller).delete();
//                log("Deleted RCs");

                Service service = client.services().inNamespace(namespace).createNew()
                        .withNewMetadata().withName("spark-service").endMetadata()
                        .withNewSpec()
                        .addNewPort().withPort(80).withNewTargetPort().withIntVal(80).endTargetPort().endPort()
                        .endSpec()
                        .done();
                log("Created service", service);

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
