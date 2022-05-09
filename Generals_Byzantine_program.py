import threading
import datetime
date_time = datetime.datetime.now()
import rpyc
import sys

# global variables
generals = {}
listofPorts = {}
primary_general_id = 0

class General(threading.Thread):
    def __init__(self, id, type, majority, state, server):
        threading.Thread.__init__(self, target=server.start)
        self.id = id
        self.type = type
        self.majority = majority
        self.state = state

    def broadcastOrder(self, order):
        self.majority = order
        secondary_general_ids = list(generals.keys())[1:]
        for secondary_general_id in secondary_general_ids:
            if secondary_general_id in listofPorts and secondary_general_id in generals:
                # connecting to thread server
                port = listofPorts[secondary_general_id]
                conn = rpyc.connect('localhost', port)
                conn.root.exposed_order(secondary_general_id, order)

    def verifyOrder(self):
        ids = list(generals.keys())
        answers_from_secondaries = []
        F_count = 0
        for gid in range(1, len(ids)):
            if ids[gid] != self.id:
                port = listofPorts[ids[gid]]
                conn = rpyc.connect('localhost', port)
                faulty, answer = conn.root.exposed_verfy_the_order(ids[gid])
                if faulty != False:
                    answers_from_secondaries.append(answer)
                elif faulty == False:
                    answers_from_secondaries.append(answer)
                    F_count += 1

        return answers_from_secondaries, F_count

    def return_undefined_state(self, default_state):
        self.majority = default_state
        secondary_general_id = self.id + 1
        if secondary_general_id <= len(generals) + 1:
            if secondary_general_id in listofPorts and secondary_general_id in generals:
                # connecting to thread server
                port = listofPorts[secondary_general_id]
                conn = rpyc.connect('localhost', port)
                conn.root.exposed_order(secondary_general_id, default_state)


class Service(rpyc.Service):
    def exposed_order(self, id, order):
        generals[id].majority = order

    def exposed_verfy_the_order(self, id):
        if generals[id].state == "F":
            return False, generals[id].majority
        else:
            return True, generals[id].majority

def sendtheorder(order):
    if len(generals) <= 3:
        failed_generals = [gn for gn in generals.values() if gn.state == "F"]
        generals[primary_general_id].majority = order
        for g in generals.values():
            print(f"G{g.id}, {g.type}, majority={g.majority}, {g.state}")
        print(f"Execute order: cannot be determined – not enough generals in the system! {len(failed_generals)} faulty node in the system - {len(generals)-1} out of {len(generals)} quorum not consistent")
    else:
        generals[primary_general_id].majority = order
        generals[primary_general_id].broadcastOrder(order)
        # verify the order
        all_answers_from_all_generals_between_messaging = []
        ids = list(generals.keys())
        failed_general_count = 0
        for gid in range(1, len(ids)):
            answers, failed_general = generals[ids[gid]].verifyOrder()
            all_answers_from_all_generals_between_messaging.append(answers)
            failed_general_count = failed_general

        shouldDoOrder = all(general_answer == all_answers_from_all_generals_between_messaging[0] for general_answer in all_answers_from_all_generals_between_messaging)

        if shouldDoOrder == True:
            for g in generals.values():
                print(f"G{g.id}, {g.type}, majority={g.majority}, {g.state}")
            if failed_general_count == 0:
                print(f"Execute order: {order}! Non-faulty nodes in the system – {len(all_answers_from_all_generals_between_messaging)} out of {len(generals)} quorum suggest {order}")
            else:
                print(f"Execute order: {order}! {failed_general_count} faulty nodes in the system – {len(all_answers_from_all_generals_between_messaging) - failed_general_count} out of {len(generals)} quorum suggest {order}")
    generals[primary_general_id].return_undefined_state("undefined")

def deleteGeneral(id):
    if id in generals and generals[id].type != "primary":
        del generals[id]
        del listofPorts[id]
    elif generals[id].type == "primary":
        # needs an election for primary general
        del generals[primary_general_id]
        del listofPorts[primary_general_id]
        election(primary_general_id + 1)
    else:
        print("The general is not exist with this id, please try again!")
    printStates()

def election(id):
    global primary_general_id
    generals[id].type = "primary"
    primary_general_id = id

def add_k_number_of_generals(K):
    last_id = list(generals.keys())[-1] + 1
    last_port = list(listofPorts.keys())[-1] + 1
    for gid in range(K):
        server = rpyc.utils.server.ThreadedServer(Service, port = last_port)
        general = General(last_id, "secondary", "undefined", "NF", server)
        generals[last_id] = general
        generals[last_id].daemon = True
        generals[last_id].start()
        listofPorts[last_id] = last_port
        last_id += 1
        last_port += 1

def createGenerals(N):
    port = 2000
    global primary_general_id
    for gid in range(1, N + 1):
        if gid == 1:
            primary_general_id = gid
            server = rpyc.utils.server.ThreadedServer(Service, port = port)
            general = General(gid, "primary", "undefined", "NF", server)
        else:
            server = rpyc.utils.server.ThreadedServer(Service, port = port)
            general = General(gid, "secondary", "undefined", "NF", server)
        generals[gid] = general
        generals[gid].daemon = True
        generals[gid].start()
        listofPorts[gid] = port
        port += 1

def printStates():
    for g in generals.values():
        print(f"G{g.id}, {g.type}, {g.state}")

def changeType(id, state):
    if state == "faulty":
        generals[id].state = "F"

def main(argument):
    N = int(argument[1])
    if N < 0:
        print("Try to run program again, N should not be below the zero")
    else:
        createGenerals(N)
        while True:
            command = input("Enter on of the commands (actual-order, g-state id faulty, g-state, g-kill id, g-add K):").lower().split(" ")
            cmd = command[0]

            if cmd == "actual-order":
                order = str(command[1])
                sendtheorder(order)
            elif cmd == "g-state":
                if len(command) > 1:
                    id = int(command[1])
                    state = command[2]
                    changeType(id, state)
                else:
                    printStates()
            elif cmd == "g-kill":
                id = int(command[1])
                deleteGeneral(id)
            elif cmd == "g-add":
                K = int(command[1])
                add_k_number_of_generals(K)

if __name__ == "__main__":
    main(sys.argv)