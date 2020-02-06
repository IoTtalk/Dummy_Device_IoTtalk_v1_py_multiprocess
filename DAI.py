import DAN
import random
import string
import time

from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from functools import partial
from multiprocessing import Process


def random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits + string.punctuation) for _ in range(length))


def run(url, d_id, d_name, data_lenth_conf, time_interval_conf):

    from numpy.random import gamma
    # shape = mean * mean / variance
    # scale = variance / mean
    d_shape = data_lenth_conf[0] * data_lenth_conf[0] / data_lenth_conf[1]
    d_scale = data_lenth_conf[1] / data_lenth_conf[0]
    d_dist = partial(gamma, d_shape, d_scale)  # data length distribution

    t_shape = time_interval_conf[0] * time_interval_conf[0] / time_interval_conf[1]
    t_scale = time_interval_conf[1] / time_interval_conf[0]
    t_dist = partial(gamma, t_shape, t_scale)  # time interval distribution

    # basic IoTtalk DA information
    DAN.profile['dm_name'] = 'Dummy_Device'
    DAN.profile['df_list'] = ['Dummy_Sensor', 'Dummy_Control']
    DAN.profile['d_name'] = d_name

    # register DA to IoTtalk
    DAN.device_registration_with_retry(url, d_id)

    while True:
        try:
            # Pull data from a device feature
            measure_time = time.time()
            value = DAN.pull('Dummy_Sensor')
            print('Pull time: {} s'.format(time.time() - measure_time))
            if value is not None:
                print("{}: {}".format(d_name, value[0]))

            # Push data to a device feature
            measure_time = time.time()
            DAN.push('Dummy_Sensor', random_string(max(int(d_dist()), 1)))
            print('Push time: {} s'.format(time.time() - measure_time))

        except Exception as e:
            print(e)
            if str(e).find('mac_addr not found:') != -1:
                print('Reg_addr is not found. Try to re-register...')
                DAN.device_registration_with_retry(url, d_id)
            else:
                print('Connection failed due to unknow reasons.')
                time.sleep(1)
        time.sleep(t_dist())


def parse_args():
    parser = ArgumentParser(description='IoTtalk DA simulator',
                            formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("url", type=str, help="The IoTtalk Server URL, need full URL (include http and port).")
    parser.add_argument("numbers", nargs='?', type=int, default=1, help="The numbers of simulator you want to create.")
    parser.add_argument("-d", "--data_length", nargs=2, metavar=("mean", "variance"), type=int, default=(1, 1),
                        help="The data (payload) length using Gamma distribution. Need mean and variance.")
    parser.add_argument("-t", "--time_interval", nargs=2, metavar=("mean", "variance"), type=int, default=(0.2, 0.04),
                        help="The interval time (second) using Gamma distribution. Need mean and variance.")
    return parser.parse_args()


def main():
    args = parse_args()

    # the random id for this client
    # avoid collision when run the same program multiple times
    random_id = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(5))

    print('id:{} numbers: {}, server: {}'.format(random_id, args.numbers, args.url))

    for i in range(args.numbers):
        p = Process(target=run,
                    args=(args.url,
                          'addr_{}_{}'.format(random_id, i + 1),
                          '{}_{}'.format(random_id, i + 1),
                          args.data_length,
                          args.time_interval))
        p.daemon = True
        p.start()
    p.join()

if __name__ == '__main__':
    main()
