"""
Course: CSE 351
Assignment: 06
Author: Sydney Ahlstrom

Justification:
I used a fully parallelized image processing pipeline with multiprocessing and queues, it processes all images from the faces folder to the step3_edges folder, and should follow the program diagram and rubric. I met all the requirements, and tried to make the code efficient.

Instructions:

- see instructions in the assignment description in Canvas

""" 

import multiprocessing as mp
import os
import cv2
import numpy as np

from cse351 import *

print(f"Current Working Directory: {os.getcwd()}")

SCRIPT_DIR= os.path.dirname(os.path.abspath(__file__))

# Folders
INPUT_FOLDER = os.path.join(SCRIPT_DIR,"faces")

STEP3_OUTPUT_FOLDER = os.path.join(SCRIPT_DIR, "step3_edges")

# Parameters for image processing
GAUSSIAN_BLUR_KERNEL_SIZE = (5, 5)
CANNY_THRESHOLD1 = 75
CANNY_THRESHOLD2 = 155

# Allowed image extensions
ALLOWED_EXTENSIONS = ['.jpg']

# ---------------------------------------------------------------------------
def create_folder_if_not_exists(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"Created folder: {folder_path}")

# ---------------------------------------------------------------------------
def task_convert_to_grayscale(image):
    if len(image.shape) == 2 or (len(image.shape) == 3 and image.shape[2] == 1):
        return image # Already grayscale
    return cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# ---------------------------------------------------------------------------
def task_smooth_image(image, kernel_size):
    return cv2.GaussianBlur(image, kernel_size, 0)

# ---------------------------------------------------------------------------
def task_detect_edges(image, threshold1, threshold2):
    if len(image.shape) == 3 and image.shape[2] == 3:
        print("Warning: Applying Canny to a 3-channel image. Converting to grayscale first for Canny.")
        image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    elif len(image.shape) == 3 and image.shape[2] != 1 : # Should not happen with typical images
        print(f"Warning: Input image for Canny has an unexpected number of channels: {image.shape[2]}")
        return image # Or raise error
    return cv2.Canny(image, threshold1, threshold2)

# ---------------------------------------------------------------------------
def process_images_in_folder(input_folder,              # input folder with images
                             output_folder,             # output folder for processed images
                             processing_function,       # function to process the image (ie., task_...())
                             load_args=None,            # Optional args for cv2.imread
                             processing_args=None):     # Optional args for processing function

    create_folder_if_not_exists(output_folder)
    print(f"\nProcessing images from '{input_folder}' to '{output_folder}'...")

    processed_count = 0
    for filename in os.listdir(input_folder):
        file_ext = os.path.splitext(filename)[1].lower()
        if file_ext not in ALLOWED_EXTENSIONS:
            continue

        input_image_path = os.path.join(input_folder, filename)
        output_image_path = os.path.join(output_folder, filename) # Keep original filename

        try:
            # Read the image
            if load_args is not None:
                img = cv2.imread(input_image_path, load_args)
            else:
                img = cv2.imread(input_image_path)

            if img is None:
                print(f"Warning: Could not read image '{input_image_path}'. Skipping.")
                continue

            # Apply the processing function
            if processing_args:
                processed_img = processing_function(img, *processing_args)
            else:
                processed_img = processing_function(img)

            # Save the processed image
            cv2.imwrite(output_image_path, processed_img)

            processed_count += 1
        except Exception as e:
            print(f"Error processing file '{input_image_path}': {e}")

    print(f"Finished processing. {processed_count} images processed into '{output_folder}'.")

# ---------------------------------------------------------------------------
def smooth_worker(input_q, output_q, kernel_size):
    while True:
        item = input_q.get()
        if item is None:
            output_q.put(None)
            break
        filename, img = item
        try:
            smoothed = task_smooth_image(img, kernel_size)
            output_q.put((filename, smoothed))
        except Exception as e:
            print(f"Error smoothing {filename}: {e}")

def grayscale_worker(input_q, output_q):
    while True:
        item = input_q.get()
        if item is None:
            output_q.put(None)
            break
        filename, img = item
        try:
            gray = task_convert_to_grayscale(img)
            output_q.put((filename, gray))
        except Exception as e:
            print(f"Error grayscale {filename}: {e}")

def edge_worker(input_q, output_folder, threshold1, threshold2):
    while True:
        item = input_q.get()
        if item is None:
            break
        filename, img = item
        try:
            edges = task_detect_edges(img, threshold1, threshold2)
            out_path = os.path.join(output_folder, filename)
            cv2.imwrite(out_path, edges)
        except Exception as e:
            print(f"Error edge {filename}: {e}")

def run_image_processing_pipeline():
    print("Starting image processing pipeline...")

    # TODO
    # - create queues
    # - create barriers
    # - create the three processes groups
    # - you are free to change anything in the program as long as you
    #   do all requirements.

    create_folder_if_not_exists(STEP3_OUTPUT_FOLDER)

    # Number of processes per stage
    N_SMOOTH = 2
    N_GRAY = 2
    N_EDGE = 2

    # Queues for pipeline
    q1 = mp.Queue(maxsize=32)
    q2 = mp.Queue(maxsize=32)
    q3 = mp.Queue(maxsize=32)

    # Start worker processes
    smoothers = [mp.Process(target=smooth_worker, args=(q1, q2, GAUSSIAN_BLUR_KERNEL_SIZE)) for _ in range(N_SMOOTH)]
    grays = [mp.Process(target=grayscale_worker, args=(q2, q3)) for _ in range(N_GRAY)]
    edges = [mp.Process(target=edge_worker, args=(q3, STEP3_OUTPUT_FOLDER, CANNY_THRESHOLD1, CANNY_THRESHOLD2)) for _ in range(N_EDGE)]

    for p in smoothers + grays + edges:
        p.start()

    # Main process: read images and enqueue to q1
    files = [f for f in os.listdir(INPUT_FOLDER) if os.path.splitext(f)[1].lower() in ALLOWED_EXTENSIONS]
    total = len(files)
    for filename in files:
        img_path = os.path.join(INPUT_FOLDER, filename)
        img = cv2.imread(img_path)
        if img is not None:
            q1.put((filename, img))
        else:
            print(f"Warning: Could not read image '{img_path}'. Skipping.")

    # Send sentinels to smoothers
    for _ in range(N_SMOOTH):
        q1.put(None)
    # Wait for smoothers to finish and propagate sentinels to grays
    for _ in range(N_SMOOTH):
        q2.get()  # Each smoother puts a None

    # Send sentinels to grays
    for _ in range(N_GRAY):
        q2.put(None)
    # Wait for grays to finish and propagate sentinels to edges
    for _ in range(N_GRAY):
        q3.get()  # Each gray puts a None

    # Send sentinels to edges
    for _ in range(N_EDGE):
        q3.put(None)

    # Join all workers
    for p in smoothers + grays + edges:
        p.join()

    print("\nImage processing pipeline finished!")
    print(f"Original images are in: '{INPUT_FOLDER}'")
    print(f"Edge images are in: '{STEP3_OUTPUT_FOLDER}'")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log = Log(show_terminal=True)
    log.start_timer('Processing Images')

    # check for input folder
    if not os.path.isdir(INPUT_FOLDER):
        print(f"Error: The input folder '{INPUT_FOLDER}' was not found.")
        print(f"Create it and place your face images inside it.")
        print('Link to faces.zip:')
        print('   https://drive.google.com/file/d/1eebhLE51axpLZoU6s_Shtw1QNcXqtyHM/view?usp=sharing')
    else:
        run_image_processing_pipeline()

    log.write()
    log.stop_timer('Total Time To complete')
