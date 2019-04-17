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
	"strconv"
	"strings"
	"fmt"
)

func bestPrice(nodes []Node) (Node, error) {
	type NodePrice struct {
		Node  Node
		Price float64
	}



	nodeList, err := getNodes()
	if err != nil {
	}
	podList, err := getPods()
	if err != nil {
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
					// return nil, err
				}
				ru := resourceUsage[p.Spec.NodeName]
				ru.CPU += cores
			}

			if strings.HasSuffix(c.Resources.Requests["memory"], "Mi") {
				millimemory := strings.TrimSuffix(c.Resources.Requests["memory"], "Mi")
				memory, err := strconv.Atoi(millimemory)
				if err != nil {
					// return nil, err
				}
				ru := resourceUsage[p.Spec.NodeName]
				ru.MEMORY += memory
			}else if strings.HasSuffix(c.Resources.Requests["memory"], "Gi") {
				millimemory := strings.TrimSuffix(c.Resources.Requests["memory"], "Gi")
				memory, err := strconv.Atoi(millimemory)
				if err != nil {
					// return nil, err
				}
				ru := resourceUsage[p.Spec.NodeName]
				ru.MEMORY += memory*1024
			}
		}
	}
	// **********
    var bestNodePrice *NodePrice
	for _, node := range nodeList.Items {
		var allocatableCores int
		var allocatablememory int 
		var err error
		if strings.HasSuffix(node.Status.Allocatable["cpu"], "m") {
			milliCores := strings.TrimSuffix(node.Status.Allocatable["cpu"], "m")
			allocatableCores, err = strconv.Atoi(milliCores)
			if err != nil {
				// return nil, err
			}
		} else {
			cpu := node.Status.Allocatable["cpu"]
			cpuFloat, err := strconv.ParseFloat(cpu, 32)
			if err != nil {
				// return nil, err
			}
			allocatableCores = int(cpuFloat * 1000)
		}  
		if strings.HasSuffix(node.Status.Allocatable["memory"], "Ki") {
			milliMemory := strings.TrimSuffix(node.Status.Allocatable["memory"], "Ki")
			allocatablememory, err = strconv.Atoi(milliMemory)
			if err != nil {
				// return nil, err
			}
			allocatablememory = int(allocatablememory/1024) 
		} else {
			memory := node.Status.Allocatable["memory"]
			memoryFloat, err := strconv.ParseFloat(memory, 32)
			if err != nil {
				// return nil, err
			}
			allocatablememory = int(memoryFloat * 1000)
		}

		freeSpace := (allocatableCores - resourceUsage[node.Metadata.Name].CPU)
		freeMemory := (allocatablememory - resourceUsage[node.Metadata.Name].MEMORY)		
		nodes = append(nodes, node)
		f:=(float64)((freeSpace+freeMemory)/2)
		// f, err := strconv.ParseFloat(price, 32)
		if err != nil {
			// return Node{}, err
		}
		if bestNodePrice == nil {
			bestNodePrice = &NodePrice{node, f}
			continue
		}
		if f > bestNodePrice.Price {
			bestNodePrice.Node = node
			bestNodePrice.Price = f
		}		
	}

	if bestNodePrice == nil {
		bestNodePrice = &NodePrice{nodes[0], 0}
	}
	fmt.Printf("%f\n",bestNodePrice.Price)
	return bestNodePrice.Node, nil
}
