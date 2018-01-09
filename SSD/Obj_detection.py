import argparse
import tools.find_mxnet
import mxnet as mx
import os
import sys
import json
import numpy as np
from detect.detector import Detector
from symbol.symbol_factory import get_symbol

def get_detector(net, prefix, epoch, data_shape, mean_pixels, ctx, num_class,
                 nms_thresh=0.5, force_nms=True, nms_topk=400):
    """
    wrapper for initialize a detector

    Parameters:
    ----------
    net : str
        test network name
    prefix : str
        load model prefix
    epoch : int
        load model epoch
    data_shape : int
        resize image shape
    mean_pixels : tuple (float, float, float)
        mean pixel values (R, G, B)
    ctx : mx.ctx
        running context, mx.cpu() or mx.gpu(?)
    num_class : int
        number of classes
    nms_thresh : float
        non-maximum suppression threshold
    force_nms : bool
        force suppress different categories
    """
    if net is not None:
        net = get_symbol(net, data_shape, num_classes=num_class, nms_thresh=nms_thresh,
            force_nms=force_nms, nms_topk=nms_topk)
    detector = Detector(net, prefix, epoch, data_shape, mean_pixels, ctx=ctx)
    return detector

def parse_args():
    parser = argparse.ArgumentParser(description='Single-shot detection network demo')
    parser.add_argument('--network', dest='network', type=str, default='resnet50',
                        help='which network to use')
    parser.add_argument('--images', dest='images', type=str, default='./data/demo/dog.jpg',
                        help='run demo with images, use comma to seperate multiple images')
    parser.add_argument('--dir', dest='dir', nargs='?',
                        help='demo image directory, optional', type=str)
    parser.add_argument('--ext', dest='extension', help='image extension, optional',
                        type=str, nargs='?')
    parser.add_argument('--epoch', dest='epoch', help='epoch of trained model',
                        default=0, type=int)
    parser.add_argument('--prefix', dest='prefix', help='trained model prefix',
                        default=os.path.join(os.getcwd(), 'model', 'ssd_'),
                        type=str)
    parser.add_argument('--cpu', dest='cpu', help='(override GPU) use CPU to detect',
                        action='store_true', default=False)
    parser.add_argument('--gpu', dest='gpu_id', type=int, default=0,
                        help='GPU device id to detect with')
    parser.add_argument('--data-shape', dest='data_shape', type=int, default=512,
                        help='set image shape')
    parser.add_argument('--mean-r', dest='mean_r', type=float, default=123,
                        help='red mean value')
    parser.add_argument('--mean-g', dest='mean_g', type=float, default=117,
                        help='green mean value')
    parser.add_argument('--mean-b', dest='mean_b', type=float, default=104,
                        help='blue mean value')
    parser.add_argument('--thresh', dest='thresh', type=float, default=0.2,
                        help='object visualize score threshold, default 0.6')
    parser.add_argument('--nms', dest='nms_thresh', type=float, default=0.5,
                        help='non-maximum suppression threshold, default 0.5')
    parser.add_argument('--force', dest='force_nms', type=bool, default=True,
                        help='force non-maximum suppression on different class')
    parser.add_argument('--timer', dest='show_timer', type=bool, default=True,
                        help='show detection time')
    parser.add_argument('--deploy', dest='deploy_net', action='store_true', default=False,
                        help='Load network from json file, rather than from symbol')
    parser.add_argument('--class-names', dest='class_names', type=str,
                        default='aeroplane, bicycle, bird, boat, bottle, bus, \
                        car, cat, chair, cow, diningtable, dog, horse, motorbike, \
                        person, pottedplant, sheep, sofa, train, tvmonitor',
                        help='string of comma separated names, or text filename')
    args = parser.parse_args()
    return args

def parse_class_names(class_names):
    """ parse # classes and class_names if applicable """
    if len(class_names) > 0:
        if os.path.isfile(class_names):
            # try to open it to read class names
            with open(class_names, 'r') as f:
                print("class_names is: ",f)
                class_names = [l.strip() for l in f.readlines()]
        else:
            class_names = [c.strip() for c in class_names.split(',')]
        for name in class_names:
            assert len(name) > 0
    else:
        raise RuntimeError("No valid class_name provided...")
    return class_names
        

def create_img_list():
    img_list = []
    total_file_num = 0
    for i in range(147,151): 
        if i < 99:         
            dir_name = "000" + str(i) + "000" + "_" + "000" + str(i+1) + "000" + "_" + str(3)
        elif i == 99:
            dir_name = "000" + str(i) + "000" + "_" + "00" + str(i+1) + "000" + "_" + str(3)
        else:
            dir_name = "00" + str(i) + "000" + "_" + "00" + str(i+1) + "000" + "_" + str(3)
        # img_list = os.listdir("/home/ubuntu/San/")
        dir_path = "/home/ubuntu/San/" + dir_name
        # print(dir_path, len(os.listdir(dir_path)))
        total_file_num += len(os.listdir(dir_path))
        for j in os.listdir(dir_path):
            img_name = dir_path + "/" + j
            img_list.append(img_name)
    print("total number of image is: ", total_file_num)
    return img_list
    
    
if __name__ == '__main__':
    args = parse_args()
    if args.cpu:
        ctx = mx.cpu()
    else:
        ctx = mx.gpu(args.gpu_id)
        
    # #load image name and path as list
    # img_list = create_img_list()
    # img_list = ['./data/demo/dog.jpg','./data/demo/eagle.jpg','./data/demo/person.jpg','./data/demo/street.jpg']
    img_list = ['/home/ubuntu/mydata/Query_image_803/0001.jpg','/home/ubuntu/mydata/Query_image_803/0560.jpg']
    # img_list = []
    # query_dir = '/home/ubuntu/mydata/Query_image_803/'
    # for each in os.listdir(query_dir):
    #     img_name = query_dir + each
    #     img_list.append(img_name)
    
    # # parse image list
    assert len(img_list) > 0, "No valid image specified to detect / used as a test word"

    network = None if args.deploy_net else args.network
    class_names = parse_class_names(args.class_names)
    if args.prefix.endswith('_'):
        prefix = args.prefix + args.network + '_' + str(args.data_shape)
    else:
        prefix = args.prefix
    detector = get_detector(network, prefix, args.epoch,
                            args.data_shape,
                            (args.mean_r, args.mean_g, args.mean_b),
                            ctx, len(class_names), args.nms_thresh, args.force_nms)
    
    # run detection

    print("detection started")
    
    detector.detect_and_visualize(img_list, args.dir, args.extension,
                                  class_names, args.thresh, args.show_timer)
    
    print("detection finished successfully!!!")


    
    
    
    
