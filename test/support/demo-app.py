# SPDX-License-Identifier: GPL-2.0-only
"""A stand-in product for the product-demo persona: a window that signs in,
loads, shows a dashboard, sits idle, then exports a report.

Usage: python3 demo-app.py <window title> <go file> <lifetime seconds>
The timeline starts when <go file> exists, so a recording can begin first.
"""
import os
import sys
import time
import tkinter as tk

TITLE, GO, LIFETIME = sys.argv[1], sys.argv[2], float(sys.argv[3])
# (seconds after go, screen). Loading and idle are the dead time a trim removes.
PLAN = [(0, "sign-in"), (3, "loading"), (11, "dashboard"), (23, "exported")]
# Not a divisor of the 2 fps trim sampling rate, so the spinner never looks
# like a two-frame alternation.
SPINNER_TURNS_PER_SECOND = 0.77

root = tk.Tk()
root.title(TITLE)
root.geometry("960x600+80+80")
canvas = tk.Canvas(root, width=960, height=600, bg="white", highlightthickness=0)
canvas.pack()
launched = time.time()
started = None


def screen(elapsed):
    current = PLAN[0][1]
    for at, name in PLAN:
        if elapsed >= at:
            current = name
    return current


def draw():
    global started
    if started is None and os.path.exists(GO):
        started = time.time()
    elapsed = 0 if started is None else time.time() - started
    canvas.delete("all")
    canvas.create_rectangle(0, 0, 960, 60, fill="#1F3A93", width=0)
    canvas.create_text(20, 30, text="Acme Dashboard", fill="white", anchor="w", font=("Helvetica", 24, "bold"))
    current = screen(elapsed)
    if current == "sign-in":
        canvas.create_text(480, 250, text="Sign in", font=("Helvetica", 40))
        canvas.create_rectangle(330, 320, 630, 370, outline="#999999", width=2)
    elif current == "loading":
        angle = (elapsed * SPINNER_TURNS_PER_SECOND * 360) % 360
        canvas.create_arc(430, 250, 530, 350, start=angle, extent=90, style="arc", width=10, outline="#1F3A93")
        canvas.create_text(480, 400, text="Loading…", font=("Helvetica", 24))
    elif current == "dashboard":
        colors = ["#E74C3C", "#F1C40F", "#2ECC71", "#3498DB", "#9B59B6"]
        for index, height in enumerate([120, 260, 180, 320, 220]):
            canvas.create_rectangle(150 + index * 140, 520 - height, 250 + index * 140, 520, fill=colors[index], width=0)
        canvas.create_text(480, 110, text="Revenue by region", font=("Helvetica", 28))
    else:
        canvas.create_rectangle(0, 60, 960, 600, fill="#2ECC71", width=0)
        canvas.create_text(480, 330, text="Report exported", fill="white", font=("Helvetica", 44, "bold"))
    if time.time() - launched > LIFETIME:
        root.destroy()
        return
    root.after(50, draw)


draw()
root.lift()
root.attributes("-topmost", True)
root.mainloop()
