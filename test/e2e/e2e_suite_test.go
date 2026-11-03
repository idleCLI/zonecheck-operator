//go:build e2e

/*
Copyright 2026 idleCLI.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/idleCLI/zonecheck-operator/test/utils"
)

const (
	managerImage = "example.com/zonecheck:v0.0.1"
	namespace    = "zonecheck-system"
)

// E2E_SKIP_DEPLOY=true -> you're running the manager yourself (make install run),
// so no image build or deploy
var skipDeploy = os.Getenv("E2E_SKIP_DEPLOY") == "true"

// needs a cluster w/ 3 zones, see test/e2e/kind-config.yaml.
// KUBECTL_KUBERC=true -> use your own kubectl config
func TestE2E(t *testing.T) {
	RegisterFailHandler(Fail)
	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)
	_, _ = fmt.Fprintf(GinkgoWriter, "Starting zonecheck e2e test suite\n")
	RunSpecs(t, "e2e suite")
}

var _ = BeforeSuite(func() {
	if os.Getenv("KUBECTL_KUBERC") != "true" {
		Expect(os.Setenv("KUBECTL_KUBERC", "false")).To(Succeed())
	}
	if skipDeploy {
		return
	}

	By("building the manager image")
	_, err := utils.Run(exec.Command("make", "docker-build", "IMG="+managerImage))
	Expect(err).NotTo(HaveOccurred())

	By("loading the manager image on Kind")
	Expect(utils.LoadImageToKindClusterWithName(managerImage)).To(Succeed())

	By("creating the manager namespace, restricted")
	_, err = utils.Run(exec.Command("kubectl", "create", "ns", namespace))
	Expect(err).NotTo(HaveOccurred())
	_, err = utils.Run(exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
		"pod-security.kubernetes.io/enforce=restricted"))
	Expect(err).NotTo(HaveOccurred())

	By("installing CRDs + deploying the manager")
	_, err = utils.Run(exec.Command("make", "install"))
	Expect(err).NotTo(HaveOccurred())
	_, err = utils.Run(exec.Command("make", "deploy", "IMG="+managerImage))
	Expect(err).NotTo(HaveOccurred())
})

var _ = AfterSuite(func() {
	if skipDeploy {
		return
	}
	_, _ = utils.Run(exec.Command("make", "undeploy", "ignore-not-found=true"))
	_, _ = utils.Run(exec.Command("make", "uninstall", "ignore-not-found=true"))
	_, _ = utils.Run(exec.Command("kubectl", "delete", "ns", namespace, "--ignore-not-found"))
})
