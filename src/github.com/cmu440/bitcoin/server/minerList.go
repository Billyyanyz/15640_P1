package main

import "fmt"

type minerWork struct {
	clientID int
	maxNonce uint64
}

type minerList struct {
	freeMiners         []int
	minerAssignedWorks map[int]minerWork
}

func newMinerList() minerList {
	return minerList{
		freeMiners:         make([]int, 0, 10),
		minerAssignedWorks: make(map[int]minerWork),
	}
}

func (ml *minerList) add(c int) {
	if _, ok := ml.minerAssignedWorks[c]; ok {
		fmt.Println("Adding ", c, " already in miner list!")
		return
	}
	ml.freeMiners = append(ml.freeMiners, c)
	ml.minerAssignedWorks[c] = minerWork{-1, 0}
}

func (ml *minerList) delete(c int) minerWork {
	w, ok := ml.minerAssignedWorks[c]
	if !ok {
		fmt.Println("Deleting ", c, " not in miner list!")
		return minerWork{-1, 0}
	}
	delete(ml.minerAssignedWorks, c)
	for i, miner := range ml.freeMiners {
		if miner == c {
			ml.freeMiners[i] = ml.freeMiners[len(ml.freeMiners)-1]
			ml.freeMiners = ml.freeMiners[:len(ml.freeMiners)-1]
			break
		}
	}
	return w
}

func (ml *minerList) check(c int) bool {
	_, ok := ml.minerAssignedWorks[c]
	return ok
}

func (ml *minerList) checkNext() bool {
	return len(ml.freeMiners) != 0
}
func (ml *minerList) getNext() int {
	LOGF.Println(len(ml.freeMiners))
	miner := ml.freeMiners[len(ml.freeMiners)-1]
	ml.freeMiners = ml.freeMiners[:len(ml.freeMiners)-1]
	return miner
}

func (ml *minerList) assignWork(miner int, work minerWork) {
	if ml.check(miner) {
		ml.minerAssignedWorks[miner] = work
	} else {
		LOGF.Printf("Trying to assign work to client!")
	}
}

func (ml *minerList) freeWork(miner int) {
	ml.minerAssignedWorks[miner] = minerWork{-1, 0}
	ml.freeMiners = append(ml.freeMiners, miner)
}

func (ml *minerList) getWorkingClient(c int) int {
	if ml.check(c) {
		return ml.minerAssignedWorks[c].clientID
	}
	return -1
}
