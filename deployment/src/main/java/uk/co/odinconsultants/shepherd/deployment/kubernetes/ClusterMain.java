package uk.co.odinconsultants.shepherd.deployment.kubernetes;

import io.fabric8.kubernetes.api.model.*;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        final String masterImage = "bde2020/spark-master:2.4.4-hadoop2.7";
        final String masterController = "spark-master";
        try (final KubernetesClient client = client(args)) {

            try (Watch watch = client.replicationControllers().inNamespace(namespace).withResourceVersion("0").watch(watcher)) {
                // Create a namespace for all our stuff
                Namespace ns = new NamespaceBuilder().withNewMetadata().withName(namespace).addToLabels("this", "rocks").endMetadata().build();
                log("Created namespace", client.namespaces().create(ns));

                // Get the namespace by name
                log("Get namespace by name", client.namespaces().withName(namespace).get());
                // Get the namespace by label
                log("Get namespace by label", client.namespaces().withLabel("this", "rocks").list());

                ResourceQuota quota = new ResourceQuotaBuilder().withNewMetadata().withName("pod-quota").endMetadata().withNewSpec().addToHard("pods", new Quantity("3")).endSpec().build();
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
                        .withNewMetadata().withName(masterController).addToLabels("app", "spark-master").endMetadata()
                        .withNewSpec().withReplicas(1)
                        .withNewTemplate()
                        .withNewMetadata().addToLabels("app", "spark-master").endMetadata()
                        .withNewSpec()
                        .addNewContainer().withName(masterController).withImage(masterImage)
                        .addNewPort().withName("sparkmastergui").withContainerPort(8080).withHostPort(8080).endPort()
                        .addNewPort().withName("sparkmastercli").withContainerPort(7077).withHostPort(7077).endPort()
                        .addToEnv(initDaemonStep)
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .endSpec().build();

                log("Created RC", client.replicationControllers().inNamespace(namespace).create(rc));

                client.replicationControllers().inNamespace(namespace).withName(masterController).scale(1);

                Thread.sleep(10000);

                final String workerController = "spark-worker-1";
                Service service = client.services().inNamespace(namespace).createNew()
                        .withNewMetadata().withName("spark-service").endMetadata()
                        .withNewSpec()
                        .addNewPort().withName("sparkgui").withPort(8080).withNewTargetPort().withIntVal(8080).endTargetPort().endPort()
                        .addNewPort().withName("sparkcli").withPort(7077).withNewTargetPort().withIntVal(7077).endTargetPort().endPort()
                        .withType("NodePort")
                        .endSpec()
                        .done();
                log("Created service", service);

                traverseEndpoints(namespace, client);

                List<Pod> pods = listPods(namespace, client);

                Pod masterPod = pods.get(0);
                String sparkMasterUrl = "spark://" + masterPod.getStatus().getHostIP() + ":7077";
                log("sparkMasterUrl", sparkMasterUrl);

                String workerImage = "bde2020/spark-worker:2.4.4-hadoop2.7";
                ReplicationController rcSlave = new ReplicationControllerBuilder()
                        .withNewMetadata().withName(workerController).addToLabels("app", "spark-worker").endMetadata()
                        .withNewSpec().withReplicas(1)
                        .withNewTemplate()
                        .withNewMetadata().addToLabels("app", "spark-worker").endMetadata()
                        .withNewSpec()
                        .addNewContainer().withName(workerController).withImage(workerImage)
                        .addNewPort().withContainerPort(8081).withHostPort(8081).endPort()
                        .addToEnv(createEnvVar("SPARK_MASTER", sparkMasterUrl))
                        .endContainer()
                        .endSpec()
                        .endTemplate()
                        .endSpec().build();


                log("Created Slave RC", client.replicationControllers().inNamespace(namespace).create(rcSlave));

                log("Root paths:", client.rootPaths());

            } finally {
                listPods(namespace, client);
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

    private static EnvVar createEnvVar(String key, String value) {
        EnvVar env = new EnvVar();
        env.setName(key);
        env.setValue(value);
        return env;
    }

    private static List<Pod> listPods(String namespace, KubernetesClient client) {
        List<Pod> pods = client.pods().inNamespace(namespace).list().getItems();
        log("pods", pods);
        for (Pod pod: pods) {
            log("name", pod.getMetadata().getName());
            log("generated name", pod.getMetadata().getGenerateName());
        }
        return pods;
    }

    private static void traverseEndpoints(String namespace, KubernetesClient client) {
        List<Endpoints> items = client.endpoints().inNamespace(namespace).list().getItems();
        log("endpoints", items);
        for (Endpoints endpoint : items) {
            List<EndpointSubset> subsets = endpoint.getSubsets();
            log("subsets", subsets);
            for (EndpointSubset subset: subsets) {
                List<EndpointAddress> addresses = subset.getAddresses();
                log("addresses", addresses);
                for (EndpointAddress address : addresses) {
                    log("hostname: ", address.getHostname());
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
