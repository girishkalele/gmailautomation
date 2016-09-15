// Go Gmail API Client to maintain a 'TODAY' label
package main

import (
	"fmt"
	"log"
	"time"

	"google.golang.org/api/gmail/v1"

	"github.com/girishkalele/gmailautomation/sdkauth"
)

var labelNameToId map[string]string
var labelIdToName map[string]string

var lookupFailures int
var modifyFailures int

func init() {
	labelNameToId = make(map[string]string)
	labelIdToName = make(map[string]string)
}

const (
	TodayLabel      = "0/Today"
	QueryTodayLabel = "0-Today"
)

// Linear search through a slice of strings for a matching string
func sliceContainsString(sl []string, searchString string) bool {
	for _, e := range sl {
		if e == searchString {
			return true
		}
	}
	return false
}

// Reverse lookup - converts a slice of label_ids to a slice of human readable labels
func prettyLabels(labels []string) []string {
	output := []string{}
	for _, e := range labels {
		translate, ok := labelIdToName[e]
		if ok {
			output = append(output, translate)
		} else {
			fmt.Println("ERROR: LABEL NOT FOUND", e)
			output = append(output, e)
		}
	}
	return output
}

// Run the given query with Q(query).Do() and asynchronously call the callback function with each message
func findItemsWithQueryAndDispatch(clientPool chan *gmail.Service, query string, callbackFn func(chan *gmail.Service, string, chan bool)) error {
	srv := <-clientPool
	defer func() { clientPool <- srv }()
	messages, err := srv.Users.Messages.List("me").Q(query).MaxResults(10000).Do()
	if err != nil {
		return err
	}
	log.Printf("Query '%s' returned %d results", query, len(messages.Messages))
	if len(messages.Messages) == 0 {
		return nil
	}
	inflight := 0
	completionChannel := make(chan bool, 1024)
	for _, m := range messages.Messages {
		go callbackFn(clientPool, m.Id, completionChannel)
		inflight++
	}
	log.Printf("Dispatched %d goroutines to handle these results asynchronously", inflight)
	// Let all the messages process asynchronously but this function returns only after all goroutines have finished
	for i := 0; i < inflight; i++ {
		<-completionChannel
	}
	log.Printf("All goroutines finished")
	return nil
}

// Format time.Time into a Gmail Query friendly date
func formatGmailDate(t time.Time) string {
	d := fmt.Sprintf("%d/%d/%d", t.Year(), t.Month(), t.Day())
	log.Printf("%v transformed to %s", t, d)
	return d
}

func cleanupOldThreads(clientPool chan *gmail.Service, threadLatestTimestamps map[string]int64, threshold int64) {
	todayLabelId := labelNameToId[TodayLabel]
	completionChannel := make(chan bool, 1024)
	inflight := 0
	for threadId, latestTimestamp := range threadLatestTimestamps {
		if latestTimestamp < threshold {
			go func(threadId string) {
				srv := <-clientPool
				defer func() { clientPool <- srv }()
				defer func() { completionChannel <- true }()
				// The latest message in this message thread is older than yesterday, remove the label
				_, err := srv.Users.Threads.Modify("me", threadId,
					&gmail.ModifyThreadRequest{RemoveLabelIds: []string{todayLabelId}}).Do()
				if err != nil {
					log.Printf("Failed to modify thread - %s", err)
					modifyFailures++
				}
			}(threadId)
			inflight++
		}
	}
	log.Printf("Dispatched %d goroutines to cleanup threads that are older than 24 hours", inflight)
	// Let all the messages process asynchronously but this function returns only after all goroutines have finished
	for i := 0; i < inflight; i++ {
		<-completionChannel
	}
	log.Printf("Cleanup complete")
}

// SyncLoop keeps running till all work is done
func doOperations(clients []*gmail.Service) {
	// We use a channel as the client pool - goroutines will block waiting for
	// an available client by reading from the channel
	clientPool := make(chan *gmail.Service, len(clients))
	for _, c := range clients {
		clientPool <- c
	}
	yesterday := time.Now().Add(-24 * time.Hour)
	rightNow := time.Now().Add(24 * time.Hour) // we need to specify the next day for the gmail filter query
	yesterdayString := formatGmailDate(yesterday)
	rightNowString := formatGmailDate(rightNow)
	// Find items with the label Today that are stale - this logic is complicated by the fact that
	// Gmail operates on "threads" - we need to confirm that there are no messages in a thread that
	// are within the last 24 hours before removing the label from all messages in the thread

	// Gmail query string - before: -24Hr label:<todaylabel>
	query := fmt.Sprintf("before:%s label:%s", yesterdayString, QueryTodayLabel)
	threadLatestTimestamps := make(map[string]int64)
	findItemsWithQueryAndDispatch(clientPool, query,
		func(clientPool chan *gmail.Service, msgId string, completionChannel chan bool) {
			// Grab a gmail.Service handle from the pool
			srv := <-clientPool
			defer func() { clientPool <- srv }()         // and return it when this function returns
			defer func() { completionChannel <- true }() // signal goroutine exited
			mfull, err := srv.Users.Messages.Get("me", msgId).Do()
			if err != nil {
				log.Printf("Failed to lookup message %s - %s\n", msgId, err)
				lookupFailures++
				return
			}
			// Need to record the message timestamp indexed by ThreadId
			latestMessageInThread := threadLatestTimestamps[mfull.ThreadId]
			if mfull.InternalDate > latestMessageInThread {
				threadLatestTimestamps[mfull.ThreadId] = mfull.InternalDate
			}
		})

	cleanupOldThreads(clientPool, threadLatestTimestamps, yesterday.Unix()*1000)

	// Gmail query string - after: -24Hr before: +24Hr
	query = fmt.Sprintf("after:%s before:%s", yesterdayString, rightNowString)
	findItemsWithQueryAndDispatch(clientPool, query,
		func(clientPool chan *gmail.Service, msgId string, completionChannel chan bool) {
			// Grab a gmail.Service handle from the pool
			srv := <-clientPool
			defer func() { clientPool <- srv }()         // and return it when this function returns
			defer func() { completionChannel <- true }() // signal goroutine exited
			mfull, err := srv.Users.Messages.Get("me", msgId).Do()
			if err != nil {
				log.Printf("Failed to lookup message %s - %s\n", msgId, err)
				lookupFailures++
				return
			}
			prettyLabels := prettyLabels(mfull.LabelIds)
			todayLabelId := labelNameToId[TodayLabel]
			if sliceContainsString(prettyLabels, TodayLabel) || mfull.InternalDate < (yesterday.Unix()*1000) {
				return
			}
			// Need to attach the Today label
			fmt.Println("Need to attach the Today label to", mfull.Id, mfull.Snippet)
			_, err = srv.Users.Threads.Modify("me", mfull.ThreadId,
				&gmail.ModifyThreadRequest{AddLabelIds: []string{todayLabelId}}).Do()
			if err != nil {
				log.Printf("Failed to modify thread - %s", err)
				modifyFailures++
			}
		})
}

// Gmail converts all human-readable labels into label ids (this probably allows labels to be renamed among other benefits)
// We need to build a translation map to convert this for use in multiple places
func buildLabelTranslationTable(clients []*gmail.Service, verbose bool) {
	srv := clients[0]
	user := "me"
	r, err := srv.Users.Labels.List(user).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve labels. %v", err)
	}
	if len(r.Labels) > 0 {
		if verbose {
			fmt.Print("Label to labelId translations:\n")
		}
		for _, l := range r.Labels {
			if verbose {
				fmt.Printf("- %-40s : %s\n", l.Name, l.Id)
			}
			labelNameToId[l.Name] = l.Id
			labelIdToName[l.Id] = l.Name
		}
	} else if verbose {
		fmt.Print("No labels found.")
	}
}

/*
* Original example main
 */
func main() {
	// Build a pool of gmail.Clients for parallel use, these share the same http.Client + OAuth2 client
	clients := sdkauth.GetGmailClients(8)
	buildLabelTranslationTable(clients, true)
	doOperations(clients)
}
