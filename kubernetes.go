// Copyright 2016 Google Inc. All Rights Reserved.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	apiHost           = "127.0.0.1:8001"
	bindingsEndpoint  = "/api/v1/namespaces/default/pods/%s/binding/"
	eventsEndpoint    = "/api/v1/namespaces/default/events"
	nodesEndpoint     = "/api/v1/nodes"
	podsEndpoint      = "/api/v1/pods"
	watchPodsEndpoint = "/api/v1/watch/pods"
)

func postEvent(event Event) error {
	var b []byte
	body := bytes.NewBuffer(b)
	err := json.NewEncoder(body).Encode(event)
	if err != nil {
		return err 
	}

	request := &http.Request{
		Body:          ioutil.NopCloser(body),
		ContentLength: int64(body.Len()),
		Header:        make(http.Header),
		Method:        http.MethodPost,
		URL: &url.URL{
			Host:   apiHost,
			Path:   eventsEndpoint,
			Scheme: "http",
		},
	}
	request.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 {
		return errors.New("Event: Unexpected HTTP status code" + resp.Status)
	}
	return nil
}

func getNodes() (*NodeList, error) {
	var nodeList NodeList

	request := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host:   apiHost,
			Path:   nodesEndpoint,
			Scheme: "http",
		},
	}
	request.Header.Set("Accept", "application/json, */*")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}

	err = json.NewDecoder(resp.Body).Decode(&nodeList)
	if err != nil {
		return nil, err
	}

	return &nodeList, nil
}

func watchUnscheduledPods() (<-chan Pod, <-chan error) {
	pods := make(chan Pod)
	errc := make(chan error, 1)

	v := url.Values{}
	v.Set("fieldSelector", "spec.nodeName=")

	request := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host:     apiHost,
			Path:     watchPodsEndpoint,
			RawQuery: v.Encode(),
			Scheme:   "http",
		},
	}
	request.Header.Set("Accept", "application/json, */*")

	go func() {
		for {
			resp, err := http.DefaultClient.Do(request)
			if err != nil {
				errc <- err
				time.Sleep(5 * time.Second)
				continue
			}

			if resp.StatusCode != 200 {
				errc <- errors.New("Invalid status code: " + resp.Status)
				time.Sleep(5 * time.Second)
				continue
			}

			decoder := json.NewDecoder(resp.Body)
			for {
				var event PodWatchEvent
				err = decoder.Decode(&event)
				if err != nil {
					errc <- err
					break
				}

				if event.Type == "ADDED" {
					pods <- event.Object
				}
			}
		}
	}()

	return pods, errc
}

func getUnscheduledPods() ([]*Pod, error) {
	var podList PodList
	unscheduledPods := make([]*Pod, 0)

	v := url.Values{}
	v.Set("fieldSelector", "spec.nodeName=")

	request := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host:     apiHost,
			Path:     podsEndpoint,
			RawQuery: v.Encode(),
			Scheme:   "http",
		},
	}
	request.Header.Set("Accept", "application/json, */*")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return unscheduledPods, err
	}
	err = json.NewDecoder(resp.Body).Decode(&podList)
	if err != nil {
		return unscheduledPods, err
	}

	for _, pod := range podList.Items {
		if pod.Metadata.Annotations["scheduler.alpha.kubernetes.io/name"] == schedulerName {
			unscheduledPods = append(unscheduledPods, &pod)
		}
	}

	return unscheduledPods, nil
}

func getPods() (*PodList, error) {
	var podList PodList

	v := url.Values{}
	v.Add("fieldSelector", "status.phase=Running")
	v.Add("fieldSelector", "status.phase=Pending")

	request := &http.Request{
		Header: make(http.Header),
		Method: http.MethodGet,
		URL: &url.URL{
			Host:     apiHost,
			Path:     podsEndpoint,
			RawQuery: v.Encode(),
			Scheme:   "http",
		},
	}
	request.Header.Set("Accept", "application/json, */*")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	err = json.NewDecoder(resp.Body).Decode(&podList)
	if err != nil {
		return nil, err
	}
	return &podList, nil
}

type ResourceUsage struct {
	CPU int
	MEMORY int
}

func fit(pods []*Pod) ([]Node, error) {
	var pod Pod
	nodeList, err := getNodes()
	if err != nil {
		return nil, err
	}

	podList, err := getPods()
	if err != nil {
		return nil, err
	}

	resourceUsage := make(map[string]*ResourceUsage)
	for _, node := range nodeList.Items {
		resourceUsage[node.Metadata.Name] = &ResourceUsage{}
	}

	for _, p := range podList.Items {
		if p.Spec.NodeName == "" {
			continue
		}
		for _, c := range p.Spec.Containers {
        	// message := fmt.Sprintf("info!!!!!!!! (%s): ", c.Resources.Requests)
        	// log.Println(message)
			if strings.HasSuffix(c.Resources.Requests["cpu"], "m") {
				milliCores := strings.TrimSuffix(c.Resources.Requests["cpu"], "m")
				cores, err := strconv.Atoi(milliCores)
				if err != nil {
					return nil, err
				}
				ru := resourceUsage[p.Spec.NodeName]
				ru.CPU += cores
			}else {
				cpu :=c.Resources.Requests["cpu"]
				cpuFloat, err := strconv.ParseFloat(cpu, 32)
				if err != nil {
					return nil, err
				}
				cpu_temp:= int(cpuFloat * 1000)
				ru := resourceUsage[p.Spec.NodeName]
				ru.CPU+=cpu_temp
			} 

			if strings.HasSuffix(c.Resources.Requests["memory"], "Mi") {
				millimemory := strings.TrimSuffix(c.Resources.Requests["memory"], "Mi")
				memory, err := strconv.Atoi(millimemory)
				if err != nil {
					return nil, err
				}
				ru := resourceUsage[p.Spec.NodeName]
				ru.MEMORY += memory
			}else if strings.HasSuffix(c.Resources.Requests["memory"], "Gi") {
				millimemory := strings.TrimSuffix(c.Resources.Requests["memory"], "Gi")
				memory, err := strconv.Atoi(millimemory)
				if err != nil {
					return nil, err
				}
				ru := resourceUsage[p.Spec.NodeName]
				ru.MEMORY += memory*1000
			}



		}
	}

	var nodes []Node
	fitFailures := make([]string, 0)


	// for _, pod := range pods {
	// 	err := schedulePod(pod)
	// 	if err != nil {
	// 		log.Println(err)
	// 	}
	// }	

	var spaceRequired int
	var memoryRequired int
	for _,pod:=range pods{
		for _, c := range pod.Spec.Containers {
			if strings.HasSuffix(c.Resources.Requests["cpu"], "m") {
				milliCores := strings.TrimSuffix(c.Resources.Requests["cpu"], "m")
				cores, err := strconv.Atoi(milliCores)
				if err != nil {
					return nil, err
				}
				spaceRequired += cores
			}else {
				cpu := c.Resources.Requests["cpu"]
				cpuFloat, err := strconv.ParseFloat(cpu, 32)
				if err != nil {
					return nil, err
				}
				spaceRequired+=int(cpuFloat * 1000)
			} 
			if strings.HasSuffix(c.Resources.Requests["memory"],"Ki"){
				millimemory:=strings.TrimSuffix(c.Resources.Requests["memory"],"Ki")
				memory,err:=strconv.Atoi(millimemory)
				if err!=nil{
					return nil,err
				}
				memoryRequired+=memory
			} else if strings.HasSuffix(c.Resources.Requests["memory"],"Gi"){
				millimemory:=strings.TrimSuffix(c.Resources.Requests["memory"],"Gi")
				memoryFloat, err := strconv.ParseFloat(millimemory, 32)
				if err!=nil{
					return nil,err
				}
				memoryRequired+=int(memoryFloat*1000)
			}else{
				millimemory:=strings.TrimSuffix(c.Resources.Requests["memory"],"Mi")
				memory,err:=strconv.Atoi(millimemory)
				if err!=nil{
					return nil,err
				}
				memoryRequired+=memory
			}
		}
	}	
// 按照job-name 来place ps-pod和worker-pod
//简单第一步：如果一个node能放下就全放在一个node下。
//一次就调度一个job，根据第一个pod的name 调度相应的ps跟worker pod。 
	for _, node := range nodeList.Items {
		var allocatableCores int
		var allocatablememory int 
		var err error
		if strings.HasSuffix(node.Status.Allocatable["cpu"], "m") {
			milliCores := strings.TrimSuffix(node.Status.Allocatable["cpu"], "m")
			allocatableCores, err = strconv.Atoi(milliCores)
			if err != nil {
				return nil, err
			}
		} else {
			cpu := node.Status.Allocatable["cpu"]
			cpuFloat, err := strconv.ParseFloat(cpu, 32)
			if err != nil {
				return nil, err
			}
			allocatableCores = int(cpuFloat * 1000)
		}  
		if strings.HasSuffix(node.Status.Allocatable["memory"], "Ki") {
			milliMemory := strings.TrimSuffix(node.Status.Allocatable["memory"], "Ki")
			allocatablememory, err = strconv.Atoi(milliMemory)
			if err != nil {
				return nil, err
			}
			allocatablememory = int(allocatablememory/1000) 
		} else if strings.HasSuffix(node.Status.Allocatable["memory"], "Gi"){
			memory := strings.TrimSuffix(node.Status.Allocatable["memory"],"Gi")
			memoryFloat, err := strconv.ParseFloat(memory, 32)
			if err != nil {
				return nil, err
			}
			allocatablememory = int(memoryFloat * 1000)
		}else{
			milliMemory:=strings.TrimSuffix(node.Status.Allocatable["memory"], "Mi")
			allocatablememory, err = strconv.Atoi(milliMemory)
			if err != nil {
				return nil, err
			}
			// allocatablememory = int(allocatablememory/1024) 
		}
		freeSpace := (allocatableCores - resourceUsage[node.Metadata.Name].CPU)
		freeMemory := (allocatablememory - resourceUsage[node.Metadata.Name].MEMORY)
        message := fmt.Sprintf("Node_name (%s): \nCPU_total:%dm CPU_free:%dm \nMEMORY_total:%dM MEMORY_free:%dM\n", node.Metadata.Name,allocatableCores,freeSpace,allocatablememory,freeMemory)
		log.Println(message)
		if freeSpace < spaceRequired || freeMemory<memoryRequired{
			m := fmt.Sprintf("fit failure on node (%s): Insufficient CPU", node.Metadata.Name)
			fitFailures = append(fitFailures, m)
			continue
		}
		//binding pod-node
		for _, pod := range pods {
			err := bind(pod,node)
			if err != nil {
				log.Println(err)
			}
		}			
		nodes = append(nodes, node)
		break
	}

	if len(nodes) == 0 {
		// Emit a Kubernetes event that the Pod was scheduled successfully.
		timestamp := time.Now().UTC().Format(time.RFC3339)
		event := Event{
			Count:          1,
			Message:        fmt.Sprintf("pod (%s) failed to fit in any node\n%s", pod.Metadata.Name, strings.Join(fitFailures, "\n")),
			Metadata:       Metadata{GenerateName: pod.Metadata.Name + "-"},
			Reason:         "FailedScheduling",
			LastTimestamp:  timestamp,
			FirstTimestamp: timestamp,
			Type:           "Warning",
			Source:         EventSource{Component: "hightower-scheduler"},
			InvolvedObject: ObjectReference{
				Kind:      "Pod",
				Name:      pod.Metadata.Name,
				Namespace: "default",
				Uid:       pod.Metadata.Uid,
			},
		}

		postEvent(event)
	}


	//binding pod and node

	return nodes, nil
}

func bind(pod *Pod, node Node) error {


	binding := Binding{
		ApiVersion: "v1",
		Kind:       "Binding",
		Metadata:   Metadata{Name: pod.Metadata.Name},
		Target: Target{
			ApiVersion: "v1",
			Kind:       "Node",
			Name:       node.Metadata.Name,
		},
	}

	var b []byte
	body := bytes.NewBuffer(b)
	err := json.NewEncoder(body).Encode(binding)
	if err != nil {
		return err
	}

	request := &http.Request{
		Body:          ioutil.NopCloser(body),
		ContentLength: int64(body.Len()),
		Header:        make(http.Header),
		Method:        http.MethodPost,
		URL: &url.URL{
			Host:   apiHost,
			Path:   fmt.Sprintf(bindingsEndpoint, pod.Metadata.Name),
			Scheme: "http",
		},
	}
	request.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	if resp.StatusCode != 201 {
		return errors.New("Binding: Unexpected HTTP status code" + resp.Status)
	}

	// Emit a Kubernetes event that the Pod was scheduled successfully.
	message := fmt.Sprintf("Successfully assigned %s to %s", pod.Metadata.Name, node.Metadata.Name)
	timestamp := time.Now().UTC().Format(time.RFC3339)
	event := Event{
		Count:          1,
		Message:        message,
		Metadata:       Metadata{GenerateName: pod.Metadata.Name + "-"},
		Reason:         "Scheduled",
		LastTimestamp:  timestamp,
		FirstTimestamp: timestamp,
		Type:           "Normal",
		Source:         EventSource{Component: "hightower-scheduler"},
		InvolvedObject: ObjectReference{
			Kind:      "Pod",
			Name:      pod.Metadata.Name,
			Namespace: "default",
			Uid:       pod.Metadata.Uid,
		},
	}
	log.Println(message)
	return postEvent(event)
}
