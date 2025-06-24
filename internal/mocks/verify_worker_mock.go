package mocks

import (
	"context"
	"fmt"
	"github.com/DIMO-Network/volteras-oracle/internal/onboarding"
	"github.com/riverqueue/river"
)

type VerifyWorkerMock struct {
	river.WorkerDefaults[onboarding.VerifyArgs]
	workedJobs []onboarding.VerifyArgs
}

func NewVerifyWorkerMock() *VerifyWorkerMock {
	return &VerifyWorkerMock{
		workedJobs: []onboarding.VerifyArgs{},
	}
}

func (w *VerifyWorkerMock) Work(_ context.Context, job *river.Job[onboarding.VerifyArgs]) error {
	w.workedJobs = append(w.workedJobs, job.Args)
	fmt.Printf("Received job: %v\n", job.Args)
	return nil
}
