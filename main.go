package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	pb "secretvm-externalgrpc/pb"
)

type secretVmCloudProvider struct {
	pb.UnimplementedCloudProviderServer

	mu          sync.Mutex
	targetSizes map[string]int32
	instances   map[string]*pb.Instance

	creationQueue []string

	minSize int32
	maxSize int32
}

type VMListResponse struct {
	Status string `json:"status"`
	Result []VM   `json:"result"`
}

type VM struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	NameFromUser string `json:"nameFromUser"`
	Status       string `json:"status"`
	State        string `json:"state"`
}

const agentComposeTemplate = `version: '3'
services:
  k3s-agent:
    image: rancher/k3s:v1.35.2-k3s1
    container_name: %s
    command: agent
    privileged: true
    restart: always
    hostname: %s
    env_file: ./.env
    environment:
      - K3S_URL=%s
`

func NewSecretVmCloudProvider(minSize, maxSize int32) *secretVmCloudProvider {
	provider := &secretVmCloudProvider{
		targetSizes:   make(map[string]int32),
		instances:     make(map[string]*pb.Instance),
		creationQueue: make([]string, 0),
		minSize:       minSize,
		maxSize:       maxSize,
	}

	go provider.creationWorkerLoop()

	return provider
}

func (s *secretVmCloudProvider) NodeGroups(ctx context.Context, req *pb.NodeGroupsRequest) (*pb.NodeGroupsResponse, error) {
	log.Println("NodeGroups")
	return &pb.NodeGroupsResponse{
		NodeGroups: []*pb.NodeGroup{
			{Id: "small", MinSize: s.minSize, MaxSize: s.maxSize},
		},
	}, nil
}

func (s *secretVmCloudProvider) NodeGroupTargetSize(ctx context.Context, req *pb.NodeGroupTargetSizeRequest) (*pb.NodeGroupTargetSizeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	size := s.targetSizes[req.Id]
	log.Printf("NodeGroupTargetSize request for %s: returning %d", req.Id, size)

	return &pb.NodeGroupTargetSizeResponse{TargetSize: size}, nil
}

func (s *secretVmCloudProvider) NodeGroupDecreaseTargetSize(ctx context.Context, req *pb.NodeGroupDecreaseTargetSizeRequest) (*pb.NodeGroupDecreaseTargetSizeResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.targetSizes[req.Id] += req.Delta
	log.Printf("DecreaseTargetSize request: Remove %d from %s", req.Delta, req.Id)

	return &pb.NodeGroupDecreaseTargetSizeResponse{}, nil
}

func (s *secretVmCloudProvider) NodeGroupForNode(ctx context.Context, req *pb.NodeGroupForNodeRequest) (*pb.NodeGroupForNodeResponse, error) {
	log.Printf("NodeGroupForNode name: %s, providerId: %s", req.Node.Name, req.Node.ProviderID)
	return &pb.NodeGroupForNodeResponse{
		NodeGroup: &pb.NodeGroup{Id: "small", MinSize: s.minSize, MaxSize: s.maxSize},
	}, nil
}

func (s *secretVmCloudProvider) NodeGroupIncreaseSize(ctx context.Context, req *pb.NodeGroupIncreaseSizeRequest) (*pb.NodeGroupIncreaseSizeResponse, error) {
	log.Printf("IncreaseSize request: Add %d nodes to %s", req.Delta, req.Id)

	domainName := os.Getenv("SECRETVM_DOMAIN_NAME")
	masterName, _, _ := strings.Cut(domainName, ".")

	s.mu.Lock()
	s.targetSizes[req.Id] += req.Delta

	for i := int32(0); i < req.Delta; i++ {
		nodeName := fmt.Sprintf("%s-worker-%s-%d", masterName, req.Id, time.Now().UnixNano())

		s.instances[nodeName] = &pb.Instance{
			Id: nodeName,
			Status: &pb.InstanceStatus{
				InstanceState: pb.InstanceStatus_instanceCreating,
			},
		}
		s.creationQueue = append(s.creationQueue, nodeName)
	}
	s.mu.Unlock()

	log.Printf("IncreaseSize returning success. %d VMs added to creation queue.", req.Delta)
	return &pb.NodeGroupIncreaseSizeResponse{}, nil
}

func (s *secretVmCloudProvider) creationWorkerLoop() {
	ticker := time.NewTicker(5 * time.Second)

	for range ticker.C {
		s.mu.Lock()
		if len(s.creationQueue) == 0 {
			s.mu.Unlock()
			continue
		}

		nodeName := s.creationQueue[0]
		s.creationQueue = s.creationQueue[1:]

		if s.instances[nodeName].Status.InstanceState == pb.InstanceStatus_instanceDeleting {
			log.Printf("Worker skipping creation for %s, it was marked for deletion.", nodeName)
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()

		s.executeVMCreation(nodeName)
	}
}

func (s *secretVmCloudProvider) executeVMCreation(nodeName string) {
	log.Printf("Worker picking up %s from queue. Starting creation...", nodeName)

	domainName := os.Getenv("SECRETVM_DOMAIN_NAME")
	apiKey := os.Getenv("SECRETVM_API_KEY")
	k3sURL := fmt.Sprintf("https://%s:6443", domainName)

	tokenBytes, err := os.ReadFile("/var/lib/rancher/k3s/server/node-token")
	if err != nil {
		log.Printf("Worker failed to read K3s node-token file: %v", err)
		s.setInstanceState(nodeName, pb.InstanceStatus_unspecified, true)
		return
	}
	k3sToken := strings.TrimSpace(string(tokenBytes))

	composeContent := fmt.Sprintf(agentComposeTemplate, nodeName, nodeName, k3sURL)

	tmpFileDockerCompose, err := os.CreateTemp("", fmt.Sprintf("agent-compose-%s-*.yaml", nodeName))
	if err != nil {
		log.Printf("Worker failed to create temp file: %v", err)
		s.setInstanceState(nodeName, pb.InstanceStatus_unspecified, true)
		return
	}

	if _, err := tmpFileDockerCompose.Write([]byte(composeContent)); err != nil {
		tmpFileDockerCompose.Close()
		os.Remove(tmpFileDockerCompose.Name())
		log.Printf("Worker failed to write temp file: %v", err)
		s.setInstanceState(nodeName, pb.InstanceStatus_unspecified, true)
		return
	}
	tmpFileDockerCompose.Close()

	tmpFileEnv, err := os.CreateTemp("", fmt.Sprintf("env-%s-*.yaml", nodeName))
	if err != nil {
		os.Remove(tmpFileDockerCompose.Name())
		log.Printf("Worker failed to create temp file: %v", err)
		s.setInstanceState(nodeName, pb.InstanceStatus_unspecified, true)
		return
	}

	envFileContent := fmt.Sprintf("K3S_TOKEN=%s\n", k3sToken)
	if _, err := tmpFileEnv.Write([]byte(envFileContent)); err != nil {
		tmpFileEnv.Close()
		os.Remove(tmpFileEnv.Name())
		os.Remove(tmpFileDockerCompose.Name())
		log.Printf("Worker failed to write temp file: %v", err)
		s.setInstanceState(nodeName, pb.InstanceStatus_unspecified, true)
		return
	}
	tmpFileEnv.Close()

	cmd := exec.Command("secretvm-cli", "-k", apiKey, "vm", "create",
		"-n", nodeName, "-t", "small", "-d", tmpFileDockerCompose.Name(), "-e", tmpFileEnv.Name())
	cmd.Env = append(os.Environ(), "SERVER_BASE_URL=https://preview-aidev.scrtlabs.com")

	output, err := cmd.CombinedOutput()
	os.Remove(tmpFileDockerCompose.Name())
	os.Remove(tmpFileEnv.Name())

	if err != nil {
		log.Printf("Worker failed to start VM %s. Error: %v, Output: %s", nodeName, err, string(output))
		s.setInstanceState(nodeName, pb.InstanceStatus_unspecified, true)
		return
	}

	log.Printf("Worker successfully triggered VM creation for %s", nodeName)
}

func (s *secretVmCloudProvider) setInstanceState(nodeName string, instanceStatus pb.InstanceStatus_InstanceState, lock bool) {
	if lock {
		s.mu.Lock()
	}
	s.instances[nodeName].Status = &pb.InstanceStatus{
		InstanceState: instanceStatus,
	}
	if lock {
		s.mu.Unlock()
	}
}

func (s *secretVmCloudProvider) NodeGroupDeleteNodes(ctx context.Context, req *pb.NodeGroupDeleteNodesRequest) (*pb.NodeGroupDeleteNodesResponse, error) {
	log.Printf("Requested to delete %d nodes.", len(req.Nodes))

	apiKey := os.Getenv("SECRETVM_API_KEY")

	cmd := exec.Command("secretvm-cli", "-k", apiKey, "vm", "ls")
	cmd.Env = append(os.Environ(), "SERVER_BASE_URL=https://preview-aidev.scrtlabs.com")

	output, err := cmd.Output()
	if err != nil {
		log.Printf("Failed to list VMs for deletion: %v. Output: %s", err, string(output))
		return nil, fmt.Errorf("failed to list vms: %v", err)
	}

	var response VMListResponse
	if err := json.Unmarshal(output, &response); err != nil {
		log.Printf("Failed to parse JSON response during deletion: %v", err)
		return nil, fmt.Errorf("failed to parse json response: %v", err)
	}

	nodeNameToID := make(map[string]string)
	for _, vm := range response.Result {
		if vm.NameFromUser != "" {
			nodeNameToID[vm.NameFromUser] = vm.ID
		}
	}

	for _, node := range req.Nodes {
		nodeName := node.Name

		s.mu.Lock()
		s.setInstanceState(nodeName, pb.InstanceStatus_instanceDeleting, false)
		s.mu.Unlock()

		vmID, exists := nodeNameToID[nodeName]
		if !exists {
			log.Printf("Warning: Node %s requested for deletion but not found via secretvm-cli. Skipping.", nodeName)
			continue
		}

		log.Printf("Deleting VM: %s (ID: %s)", nodeName, vmID)

		deleteCmd := exec.Command("secretvm-cli", "-k", apiKey, "vm", "remove", vmID)
		deleteCmd.Env = append(os.Environ(), "SERVER_BASE_URL=https://preview-aidev.scrtlabs.com")

		deleteOutput, deleteErr := deleteCmd.CombinedOutput()
		if deleteErr != nil {
			log.Printf("Failed to delete VM %s (ID: %s). Error: %v, Output: %s", nodeName, vmID, deleteErr, string(deleteOutput))
			continue
		}

		s.mu.Lock()
		delete(s.instances, nodeName)
		s.mu.Unlock()
	}

	s.mu.Lock()
	s.targetSizes[req.Id] -= int32(len(req.Nodes))
	s.mu.Unlock()

	return &pb.NodeGroupDeleteNodesResponse{}, nil
}

func (s *secretVmCloudProvider) NodeGroupNodes(ctx context.Context, req *pb.NodeGroupNodesRequest) (*pb.NodeGroupNodesResponse, error) {
	domainName := os.Getenv("SECRETVM_DOMAIN_NAME")
	apiKey := os.Getenv("SECRETVM_API_KEY")
	cmd := exec.Command("secretvm-cli", "-k", apiKey, "vm", "ls")
	cmd.Env = append(os.Environ(), "SERVER_BASE_URL=https://preview-aidev.scrtlabs.com")

	output, err := cmd.Output()
	if err != nil {
		log.Printf("Failed to list VMs: %v. Output: %s", err, string(output))
		return nil, fmt.Errorf("failed to list vms: %v", err)
	}

	var response VMListResponse
	if err := json.Unmarshal(output, &response); err != nil {
		log.Printf("Failed to parse JSON response: %v", err)
		return nil, fmt.Errorf("failed to parse json response: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	masterName, _, _ := strings.Cut(domainName, ".")
	for _, vm := range response.Result {
		if !strings.HasPrefix(vm.NameFromUser, fmt.Sprintf("%s-worker-%s", masterName, req.Id)) {
			continue
		}

		if _, exists := s.instances[vm.NameFromUser]; !exists {
			log.Printf("Warning: node %s doesn't exist in the internal state. This is definitely a bug.", vm.NameFromUser)
			continue
		}

		if s.instances[vm.NameFromUser].Status.InstanceState == pb.InstanceStatus_instanceDeleting {
			log.Printf("Sweeper spotted Ghost VM %s. Initiating cleanup.", vm.NameFromUser)
			go s.asyncCleanupGhostVM(vm.NameFromUser, vm.ID)

			continue
		}

		if vm.State == "running" {
			s.setInstanceState(vm.NameFromUser, pb.InstanceStatus_instanceRunning, false)
		}
	}

	var instances []*pb.Instance
	for name, inst := range s.instances {
		if strings.HasPrefix(name, fmt.Sprintf("%s-worker-%s", masterName, req.Id)) {
			// Return a copy to avoid race conditions on the pointer
			instances = append(instances, &pb.Instance{
				Id: inst.Id,
				Status: &pb.InstanceStatus{
					InstanceState: inst.Status.InstanceState,
				},
			})
		}
	}

	log.Printf("NodeGroupNodes found %d total instances in state map for group %s", len(instances), req.Id)
	return &pb.NodeGroupNodesResponse{Instances: instances}, nil
}

func (s *secretVmCloudProvider) asyncCleanupGhostVM(nodeName string, vmID string) {
	apiKey := os.Getenv("SECRETVM_API_KEY")
	cmd := exec.Command("secretvm-cli", "-k", apiKey, "vm", "remove", vmID)
	cmd.Env = append(os.Environ(), "SERVER_BASE_URL=https://preview-aidev.scrtlabs.com")

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("Sweeper failed to delete ghost VM %s: %v, Output: %s", nodeName, err, string(output))
		return
	}

	log.Printf("Sweeper successfully destroyed ghost VM %s", nodeName)

	s.mu.Lock()
	delete(s.instances, nodeName)
	s.mu.Unlock()
}

func (s *secretVmCloudProvider) Refresh(ctx context.Context, req *pb.RefreshRequest) (*pb.RefreshResponse, error) {
	log.Println("Refresh")
	return &pb.RefreshResponse{}, nil
}

func (s *secretVmCloudProvider) NodeGroupGetOptions(ctx context.Context, req *pb.NodeGroupAutoscalingOptionsRequest) (*pb.NodeGroupAutoscalingOptionsResponse, error) {
	options, ok := proto.Clone(req.GetDefaults()).(*pb.NodeGroupAutoscalingOptions)
	if !ok || options == nil {
		options = &pb.NodeGroupAutoscalingOptions{}
	}

	if envVal, exists := os.LookupEnv("SCALE_DOWN_UNNEEDED_DURATION"); exists && envVal != "" {
		if parsedDuration, err := time.ParseDuration(envVal); err == nil {
			options.ScaleDownUnneededDuration = durationpb.New(parsedDuration)
		}
	}

	if envVal, exists := os.LookupEnv("MAX_NODE_PROVISION_TIME"); exists && envVal != "" {
		if parsedDuration, err := time.ParseDuration(envVal); err == nil {
			options.MaxNodeProvisionDuration = durationpb.New(parsedDuration)
		}
	}

	marshaller := protojson.MarshalOptions{
		Multiline:       true,
		Indent:          "  ",
		EmitUnpopulated: true,
	}

	if jsonBytes, err := marshaller.Marshal(options); err == nil {
		log.Printf("DEBUG: %s", string(jsonBytes))
	} else {
		log.Printf("DEBUG: Failed to format options for logging: %v", err)
	}

	return &pb.NodeGroupAutoscalingOptionsResponse{
		NodeGroupAutoscalingOptions: options,
	}, nil
}

func (s *secretVmCloudProvider) GPULabel(ctx context.Context, req *pb.GPULabelRequest) (*pb.GPULabelResponse, error) {
	log.Println("GPULabel")
	return &pb.GPULabelResponse{Label: "gpu"}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":8888")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	minSize := os.Getenv("NODES_MIN_SIZE")
	if minSize == "" {
		minSize = "1"
	}
	minSizeInt64, err := strconv.ParseInt(minSize, 10, 32)
	if err != nil {
		fmt.Printf("Failed to parse minSize: %v\n", err)
		return
	}
	maxSize := os.Getenv("NODES_MAX_SIZE")
	if maxSize == "" {
		maxSize = "3"
	}
	maxSizeInt64, err := strconv.ParseInt(maxSize, 10, 32)
	if err != nil {
		fmt.Printf("Failed to parse maxSize: %v\n", err)
		return
	}
	pb.RegisterCloudProviderServer(grpcServer, NewSecretVmCloudProvider(int32(minSizeInt64), int32(maxSizeInt64)))

	log.Println("SecretVm gRPC Cloud Provider listening on :8888")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
