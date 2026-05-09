package main

import (
	"context"
	"crypto/tls"
	"fmt"

	"bitbucket.org/henrycg/riposte/db"
	"bitbucket.org/henrycg/riposte/utils"
)

type rpcCoordinatorAdmin struct {
	addr string
}

func (c rpcCoordinatorAdmin) ApplyScalingRecommendation(ctx context.Context, dryRun bool) (db.ApplyScalingRecommendationReply, error) {
	type result struct {
		reply db.ApplyScalingRecommendationReply
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		certs := []tls.Certificate{utils.LeaderCertificate}
		client, err := utils.DialHTTPWithTLS("tcp", c.addr, -1, certs)
		if err != nil {
			ch <- result{err: err}
			return
		}
		defer client.Close()
		var reply db.ApplyScalingRecommendationReply
		err = client.Call("Server.ApplyScalingRecommendation", &db.ApplyScalingRecommendationArgs{DryRun: dryRun}, &reply)
		ch <- result{reply: reply, err: err}
	}()

	select {
	case <-ctx.Done():
		return db.ApplyScalingRecommendationReply{}, fmt.Errorf("coordinator apply scaling recommendation timeout: %w", ctx.Err())
	case result := <-ch:
		return result.reply, result.err
	}
}
