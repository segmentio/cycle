package aws

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/autoscaling"
	"github.com/aws/aws-sdk-go/service/autoscaling/autoscalingiface"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/aws/aws-sdk-go/service/ecs"
	"github.com/aws/aws-sdk-go/service/ecs/ecsiface"
	"github.com/segmentio/cycle"
	errors "github.com/segmentio/errors-go"
	_ "github.com/segmentio/errors-go/awserrors"
)

type Environment struct {
	asg autoscalingiface.AutoScalingAPI
	ec2 ec2iface.EC2API
	ecs ecsiface.ECSAPI

	mutex sync.RWMutex
	cache map[cycle.InstanceID]cachedInstance
}

type cachedInstance struct {
	ec2InstanceId  string
	ec2GroupName   string
	ec2TermHook    string
	ecsInstanceArn string
	ecsCluster     string
	drainStartedAt time.Time
}

const (
	cycleInstanceUpdatedAt = "cycle.Instance.UpdatedAt"
)

func NewEnvironment(session *session.Session) *Environment {
	return &Environment{
		asg: autoscaling.New(session),
		ec2: ec2.New(session),
		ecs: ecs.New(session),
	}
}

func (env *Environment) LookupClusterID(ctx context.Context, name string) (cycle.ClusterID, error) {
	g, err := env.describeAutoScalingGroup(ctx, name)
	if err != nil {
		return "", err
	}
	return cycle.ClusterID(aws.StringValue(g.AutoScalingGroupName)), nil
}

func (env *Environment) DescribeCluster(ctx context.Context, id cycle.ClusterID) (cycle.Cluster, error) {
	groupName := string(id)

	group, err := env.describeAutoScalingGroup(ctx, groupName)
	if err != nil {
		return cycle.Cluster{}, err
	}

	hooks, err := env.describeLifecycleHooks(ctx, groupName)
	if err != nil {
		return cycle.Cluster{}, err
	}
	termHook := ""
	for _, hook := range hooks {
		hookType := aws.StringValue(hook.LifecycleTransition)
		hookName := aws.StringValue(hook.LifecycleHookName)
		if hookType == "autoscaling:EC2_INSTANCE_TERMINATING" {
			termHook = hookName
			break
		}
	}

	cluster := cycle.Cluster{
		ID:        id,
		Name:      groupName,
		Config:    cycle.ConfigID(aws.StringValue(group.LaunchConfigurationName)),
		MinSize:   int(aws.Int64Value(group.MinSize)),
		MaxSize:   int(aws.Int64Value(group.MaxSize)),
		Instances: make([]cycle.Instance, 0, len(group.Instances)),
	}

	instanceCache := make(map[cycle.InstanceID]cachedInstance, len(group.Instances))
	instanceMap := make(map[string]cycle.Instance, len(group.Instances))
	instanceIds := make([]*string, 0, len(group.Instances))

	for _, instance := range group.Instances {
		if aws.StringValue(instance.HealthStatus) != "Healthy" {
			continue
		}

		var id = aws.StringValue(instance.InstanceId)
		var config = aws.StringValue(instance.LaunchConfigurationName)
		var state cycle.InstanceState

		switch aws.StringValue(instance.LifecycleState) {
		case "Pending", "Pending:Wait", "Pending:Proceed":
			state = cycle.Starting
		case "InService", "Terminating", "Terminating:Wait":
			state = cycle.Started
		case "Terminating:Proceed":
			state = cycle.Terminating
		default: // "Terminated", "Standby", ...
			continue
		}

		instanceMap[id] = cycle.Instance{
			ID:     cycle.InstanceID(id),
			Config: cycle.ConfigID(config),
			State:  state,
		}

		instanceCache[cycle.InstanceID(id)] = cachedInstance{
			ec2InstanceId: aws.StringValue(instance.InstanceId),
			ec2GroupName:  groupName,
			ec2TermHook:   termHook,
		}

		instanceIds = append(instanceIds, instance.InstanceId)
	}

	ec2Instances, err := env.describeInstances(ctx, instanceIds)
	ec2InstancesNeedTag := []*string{}
	if err != nil {
		return cycle.Cluster{}, err
	}
	for _, ec2Instance := range ec2Instances {
		id := aws.StringValue(ec2Instance.InstanceId)
		instance, ok := instanceMap[id]
		if !ok {
			continue
		}

		for _, tag := range ec2Instance.Tags {
			key := aws.StringValue(tag.Key)
			val := aws.StringValue(tag.Value)
			if key == cycleInstanceUpdatedAt {
				instance.UpdatedAt, _ = time.Parse(time.RFC3339, val)
				break
			}
		}

		if instance.UpdatedAt.IsZero() {
			instance.UpdatedAt = time.Now()
			ec2InstancesNeedTag = append(ec2InstancesNeedTag, ec2Instance.InstanceId)
		}

		instance.CreatedAt = aws.TimeValue(ec2Instance.LaunchTime)
		instanceMap[id] = instance
	}

	clusterName := groupName
	for _, tag := range group.Tags {
		key := aws.StringValue(tag.Key)
		val := aws.StringValue(tag.Value)
		if key == "Cluster" {
			clusterName = val
			break
		}
	}

	containerInstances, err := env.describeContainerInstances(ctx, clusterName)
	if err != nil {
		return cycle.Cluster{}, err
	}
	for _, containerInstance := range containerInstances {
		id := aws.StringValue(containerInstance.Ec2InstanceId)
		instance, ok := instanceMap[id]
		if !ok {
			continue
		}

		if aws.StringValue(containerInstance.Status) == "DRAINING" {
			taskCount := 0 +
				aws.Int64Value(containerInstance.RunningTasksCount) +
				aws.Int64Value(containerInstance.PendingTasksCount)
			if taskCount == 0 {
				instance.State = cycle.Drained
			} else {
				instance.State = cycle.Draining
			}
		}

		cachedInstance := instanceCache[instance.ID]
		cachedInstance.ecsInstanceArn = aws.StringValue(containerInstance.ContainerInstanceArn)
		cachedInstance.ecsCluster = clusterName
		instanceCache[instance.ID] = cachedInstance

		instanceMap[id] = instance
	}

	for id, instance := range instanceMap {
		for _, containerInstance := range containerInstances {
			if aws.StringValue(containerInstance.Ec2InstanceId) == string(id) {
				id = ""
				break
			}
		}
		if len(id) != 0 {
			instance.State = cycle.Drained
			instanceMap[id] = instance
		}
	}

	if len(ec2InstancesNeedTag) != 0 {
		env.createCycleInstanceUpdatedAtTag(ctx, time.Now(), ec2InstancesNeedTag...)
	}

	for _, instance := range instanceMap {
		cluster.Instances = append(cluster.Instances, instance)
	}

	env.mutex.Lock()
	env.cache = instanceCache
	env.mutex.Unlock()
	return cluster, nil
}

func (env *Environment) StartInstances(ctx context.Context, cluster cycle.ClusterID, count int) error {
	groupName := string(cluster)

	group, err := env.describeAutoScalingGroup(ctx, groupName)
	if err != nil {
		return err
	}

	_, err = env.asg.SetDesiredCapacityWithContext(ctx, &autoscaling.SetDesiredCapacityInput{
		AutoScalingGroupName: group.AutoScalingGroupName,
		DesiredCapacity:      aws.Int64(int64(len(group.Instances) + count)),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (env *Environment) DrainInstance(ctx context.Context, instance cycle.InstanceID) error {
	env.mutex.RLock()
	cachedInstance, ok := env.cache[instance]
	env.mutex.RUnlock()
	if !ok {
		return errors.WithTypes(errors.Errorf("no instance with id %s was found", instance), "NotFound")
	}

	_, err := env.ecs.UpdateContainerInstancesStateWithContext(ctx, &ecs.UpdateContainerInstancesStateInput{
		ContainerInstances: []*string{aws.String(cachedInstance.ecsInstanceArn)},
		Cluster:            aws.String(cachedInstance.ecsCluster),
		Status:             aws.String("DRAINING"),
	})
	if err != nil {
		return errors.WithStack(err)
	}

	return env.createCycleInstanceUpdatedAtTag(ctx, time.Now(), aws.String(string(instance)))
}

func (env *Environment) TerminateInstance(ctx context.Context, instance cycle.InstanceID) error {
	env.mutex.RLock()
	cachedInstance, ok := env.cache[instance]
	env.mutex.RUnlock()
	if !ok {
		return errors.WithTypes(errors.Errorf("no instance with id %s was found", instance), "NotFound")
	}

	_, err := env.asg.TerminateInstanceInAutoScalingGroupWithContext(ctx, &autoscaling.TerminateInstanceInAutoScalingGroupInput{
		InstanceId:                     aws.String(string(instance)),
		ShouldDecrementDesiredCapacity: aws.Bool(true),
	})
	if err != nil {
		// Retry without decrementing the count in case we're going under the min.
		_, err := env.asg.TerminateInstanceInAutoScalingGroupWithContext(ctx, &autoscaling.TerminateInstanceInAutoScalingGroupInput{
			InstanceId:                     aws.String(string(instance)),
			ShouldDecrementDesiredCapacity: aws.Bool(false),
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	_, err = env.asg.CompleteLifecycleActionWithContext(ctx, &autoscaling.CompleteLifecycleActionInput{
		AutoScalingGroupName:  aws.String(cachedInstance.ec2GroupName),
		InstanceId:            aws.String(cachedInstance.ec2InstanceId),
		LifecycleHookName:     aws.String(cachedInstance.ec2TermHook),
		LifecycleActionResult: aws.String("CONTINUE"),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (env *Environment) WaitInstanceState(ctx context.Context, instance cycle.InstanceID, state cycle.InstanceState) error {
	return ctx.Err()
}

func (env *Environment) createCycleInstanceUpdatedAtTag(ctx context.Context, updatedAt time.Time, instanceIds ...*string) error {
	_, err := env.ec2.CreateTagsWithContext(ctx, &ec2.CreateTagsInput{
		Resources: instanceIds,
		Tags: []*ec2.Tag{{
			Key:   aws.String(cycleInstanceUpdatedAt),
			Value: aws.String(updatedAt.Format(time.RFC3339)),
		}},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (env *Environment) describeAutoScalingGroup(ctx context.Context, name string) (*autoscaling.Group, error) {
	out, err := env.asg.DescribeAutoScalingGroupsWithContext(ctx, &autoscaling.DescribeAutoScalingGroupsInput{
		AutoScalingGroupNames: []*string{aws.String(name)},
		MaxRecords:            aws.Int64(1),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return out.AutoScalingGroups[0], nil
}

func (env *Environment) describeLifecycleHooks(ctx context.Context, name string) ([]*autoscaling.LifecycleHook, error) {
	out, err := env.asg.DescribeLifecycleHooksWithContext(ctx, &autoscaling.DescribeLifecycleHooksInput{
		AutoScalingGroupName: aws.String(name),
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return out.LifecycleHooks, nil
}

func (env *Environment) describeInstances(ctx context.Context, instanceIds []*string) ([]*ec2.Instance, error) {
	var instances = make([]*ec2.Instance, 0, len(instanceIds))
	var nextToken *string
	for {
		out, err := env.ec2.DescribeInstancesWithContext(ctx, &ec2.DescribeInstancesInput{
			InstanceIds: instanceIds,
			NextToken:   nextToken,
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}

		for _, ec2Reservation := range out.Reservations {
			for _, ec2Instance := range ec2Reservation.Instances {
				instances = append(instances, ec2Instance)
			}
		}

		instanceIds = nil
		nextToken = out.NextToken

		if nextToken == nil {
			return instances, nil
		}
	}
}

func (env *Environment) describeContainerInstances(ctx context.Context, cluster string) ([]*ecs.ContainerInstance, error) {
	containerInstanceArns, err := env.listContainerInstances(ctx, cluster)
	if err != nil {
		return nil, err
	}
	out, err := env.ecs.DescribeContainerInstancesWithContext(ctx, &ecs.DescribeContainerInstancesInput{
		Cluster:            aws.String(cluster),
		ContainerInstances: containerInstanceArns,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return out.ContainerInstances, nil
}

func (env *Environment) listContainerInstances(ctx context.Context, cluster string) ([]*string, error) {
	var containerInstanceArns = make([]*string, 0, 100)
	var clusterName = aws.String(cluster)
	var nextToken *string
	for {
		out, err := env.ecs.ListContainerInstancesWithContext(ctx, &ecs.ListContainerInstancesInput{
			Cluster:   clusterName,
			NextToken: nextToken,
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}

		containerInstanceArns = append(containerInstanceArns, out.ContainerInstanceArns...)
		clusterName = nil
		nextToken = out.NextToken

		if nextToken == nil {
			return containerInstanceArns, nil
		}
	}
}
