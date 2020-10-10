package main

import (
	"container/heap"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/vonage/vonage-go-sdk"
)

//User struct
type User struct {
	Id          int
	PhoneNumber string
	Userstatus  bool
}

var allUsers []User

// Constructing Priority queue..............................................................................

//Item struct
type Item struct {
	value    string
	priority int
	reciever string
	index    int // The index of the item in the heap.
}

// A PriorityQueue holds items
type PriorityQueue []*Item

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].priority > pq[j].priority
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

//Push adds a new item to queue
func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*Item)
	item.index = n
	*pq = append(*pq, item)
}

//Pop gets the item with highest priority from queue
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

func (pq *PriorityQueue) update(item *Item, value string, priority int, reciever string) {
	item.value = value
	item.priority = priority
	item.reciever = reciever
	heap.Fix(pq, item.index)
}

//Constructing Priority queue..............................................................................

//FetchAllUsers gets all users ...................................................................
func FetchAllUsers() []User {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1)/sms_messaging")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	rows, _ := db.Query("SELECT Id, PhoneNumber, Status FROM users")
	var id int
	var phonenumber string
	var status bool
	var users []User

	for rows.Next() {
		err := rows.Scan(&id, &phonenumber, &status)
		if err != nil {
			panic(err.Error())
		}
		if !status {
			continue
		}
		users = append(users, User{Id: id, PhoneNumber: phonenumber, Userstatus: status})
	}
	return users
}

//............................................................................................

//SendMessage - function for sending messages..................................................
func SendMessage(clientnumber string, messageInfo string) {

	//sms messa
	api_Key := "c54c7077"
	api_Secret := "gYkxLPk4zwc1pJU3"
	auth := vonage.CreateAuthFromKeySecret(api_Key, api_Secret)
	smsClient := vonage.NewSMSClient(auth)
	response, _ := smsClient.Send("Vonage APIs", clientnumber, messageInfo, vonage.SMSOpts{Type: "unicode"})

	if response.Messages[0].Status == "0" {
		fmt.Println("Message sent")
	}

}

//..............................................................................................

//SendToAllUsers sends message to all users.....................................................
func SendToAllUsers(text string) {
	for _, people := range allUsers {
		SendMessage(people.PhoneNumber, text)
	}
}

//..............................................................................................

//WorkOnMessages get messages...............................................................
func WorkOnMessages() {
	db, err := sql.Open("mysql", "root:@tcp(127.0.0.1)/sms_messaging")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	rows, _ := db.Query("SELECT text, reciever FROM messages")
	var text string
	var recievertype string
	messagesqueue := make(PriorityQueue, 0)
	heap.Init(&messagesqueue)
	i := 1
	for rows.Next() {
		err := rows.Scan(&text, &recievertype)
		if err != nil {
			panic(err.Error())
		}
		messagepriority := 100
		if recievertype != "allusers" {
			messagepriority = 200
		}
		item := &Item{value: text, priority: messagepriority, reciever: recievertype, index: i}
		heap.Push(&messagesqueue, item)
		i++
	}

	for messagesqueue.Len() > 0 {
		item := heap.Pop(&messagesqueue).(*Item)
		if item.reciever != "all" {
			SendMessage(item.reciever, item.value)
			continue
		}
		SendToAllUsers(item.value)
	}
}

//.............................................................................................

func main() {
	allUsers = FetchAllUsers()
	WorkOnMessages()
}
