from tkinter import Tk, Label, Button, Entry
from Db_worker import DB_worker
import sqlite3
from datetime import datetime


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
    def start(self):
        self.new_batch_id = DB_worker.new_batch(connection=self.connection, start_date=datetime.now(),
                                                description=self.batch_desc)

    # TODO: Stop FIFO reading, finish parsing and close batch
    def stop(self):
        DB_worker.close_batch(connection=self.connection, batch_id=self.new_batch_id, stop_date=datetime.now())

root = Tk()
my_gui = BackendGUI(root)
root.mainloop()
