from tkinter import Tk, Label, Button, Entry
from Db_worker import DB_worker
import sqlite3
from datetime import datetime
import Queue
import time
import threading


class BackendGUI:
    def __init__(self, master):
        self.connection = sqlite3.connect("../../measured_data.db")
        self.batch_desc = ''
        self.new_batch_id = -100

        self.master = master
        master.title("DataLogger")

        self.label = Label(master, text="Enter the description of the batch and click Start.")
        self.label.pack()

        vcmd = master.register(self.validate)  # we have to wrap the command
        self.entry = Entry(master, validate="key", validatecommand=(vcmd, '%P'))
        self.entry.pack()

        self.start_button = Button(master, text="Start", command=self.start)
        self.start_button.pack()

        self.stop_button = Button(master, text="Close", command=self.stop)
        self.stop_button.pack()

    def validate(self, new_text):
        if len(new_text) > 150 or len(new_text) == 0:
            raise ValueError('Max. length of the description is 150 char.')
        else:
            self.batch_desc = new_text
        return True

    # TODO: Start here two threads FIFO and Parser
    # TODO: set Start button enable = false
    def start(self):
        self.new_batch_id = DB_worker.new_batch(connection=self.connection, start_date=datetime.now(),
                                                description=self.batch_desc)
        self.message_queue = Queue.Queue()
        self.can_read = True

        self.t1 = threading.Thread(target=self.read_from_bluetooth, args=[self])
        self.t2 = threading.Thread(target=self.parse_message, args=(self,))

        self.t1.start()
        self.t2.start()

    # TODO: Stop FIFO reading, finish parsing and close batch
    # TODO: set Stop button enable = false
    def stop(self):
        self.can_read = False
        while not self.message_queue.empty():
            time.sleep(0.1)
        DB_worker.close_batch(connection=self.connection, batch_id=self.new_batch_id, stop_date=datetime.now())

    # TODO: what wrong with the 2nd argument??? (read_from_bluetooth() takes exactly 1 argument (2 given))
    def read_from_bluetooth(self, event):
        i = 0
        while self.can_read:
            i += 1
            self.message_queue.put('some_data ' + str(i))  # TODO: message from bluetooth
            print 'Written ' + str(i)
            time.sleep(1)

    # TODO: what wrong with the 2nd argument??? (read_from_bluetooth() takes exactly 1 argument (2 given))
    def parse_message(self, event):
        while self.can_read or not self.message_queue.empty():
            message = self.message_queue.get() # TODO: parse the message
            print message

root = Tk()
my_gui = BackendGUI(root)
root.mainloop()
