extern crate rand;
use rand::Rng;
//use rand::Buf;
extern crate bytes;
use bytes::Buf;

use std::env;
use std::f32;
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        println!("Usage: {} <threads> input output", args[0]);
    }

    let threads = args[1].parse::<usize>().unwrap();
    let inp_path = &args[2];
    let out_path = &args[3];

    // Sample
    // Calculate pivots
    let mut inpf = File::open(inp_path).unwrap();
    let size = read_size(&mut inpf);

    let pivots = find_pivots(&mut inpf, threads, size);

    // Create output file
    {
        let mut outf = File::create(out_path).unwrap();
        let tmp = size.to_ne_bytes();
        outf.write_all(&tmp).unwrap();
        outf.set_len(size).unwrap();
    }

    let mut workers = vec![];

    // Spawn worker threads
    let sizes = Arc::new(Mutex::new(vec![0u64; threads]));
    let barrier = Arc::new(Barrier::new(threads));

    for ii in 0..threads {
        let inp = inp_path.clone();
        let out = out_path.clone();
        let piv = pivots.clone();
        let szs = sizes.clone();
        let bar = barrier.clone();

        let tt = thread::spawn(move || {
            worker(ii, inp, out, piv, szs, bar);
        });
        workers.push(tt);
    }

    // Join worker threads
    for tt in workers {
        tt.join().unwrap();
    }
}

fn read_size(file: &mut File) -> u64 {
    // TODO: Read size field from data file
    //file.seek(SeekFrom::Start(8)).unwrap();
    let mut temp = [0u8; 8];
    file.read_exact(&mut temp).unwrap();
    let size = Cursor::new(temp).get_u64_le();
    //println!("{}\n", size);
    size
}

fn read_item(file: &mut File, ii: u64) -> f32 {
    // TODO: Read the ii'th float from data file
    file.seek(SeekFrom::Start(0)).unwrap();
    let mut temp = [0u8; 4];
    file.seek(SeekFrom::Start(8+(4*ii))).unwrap();
    file.read_exact(&mut temp).unwrap();
    let item = Cursor::new(temp).get_f32_le();
    item
}

fn sample(file: &mut File, count: usize, size: u64) -> Vec<f32> {
    let mut rng = rand::thread_rng();

    let mut ys = vec![];
   // file.seek(SeekFrom::Start(8)).unwrap();
    for _ii in 0..count {
        ys.push(read_item(file,rng.gen_range(0,size-1)));
    }
    // TODO: Sample 'count' random items from the
    // provided file
    //println!("samples\n");
    //println!("{:?}",ys);
    ys
}

fn find_pivots(file: &mut File, threads: usize, size: u64) -> Vec<f32> {
    // TODO: Sample 3*(threads-1) items from the file
    let mut pivots = vec![0f32];
    
    let mut samples = sample(file,3*(threads-1),size);
    //pivots.push(0.0);
    // TODO: Sort the sampled list
    samples.sort_by(|a,b| a.partial_cmp(b).unwrap());
    //println!("\nsamples after sort\n");
    //println!("{:?}",samples);
    //pivots.push(samples[1]);
    for i in (1..samples.len()).step_by(3){
        pivots.push(samples[i]);
    }
    // TODO: push the pivots into the array


    pivots.push(f32::INFINITY);
    //println!("\nPivots:\n");
    //println!("{:?}",pivots);
    pivots
}

fn worker(
    tid: usize,
    inp_path: String,
    out_path: String,
    //size: u64,
    pivots: Vec<f32>,
    sizes: Arc<Mutex<Vec<u64>>>,
    bb: Arc<Barrier>,
) {
    // TODO: Open input as local fh
    let mut inpf = File::open(inp_path).unwrap();
    //initializing local data vector
    let mut data = vec![];
    let size =  read_size(&mut inpf);
    //let mut temp = [0u8; 4];
    //inpf.seek(SeekFrom::Start(8)).unwrap();
    // TODO: Scan to collect local data
    //let mut sizes_local = 0u64;
    for i in 0..size{
        //inpf.read_exact(&mut temp).unwrap();
        //let item = Cursor::new(temp).get_f32_le();
        let item = read_item(&mut inpf,i);
        if (item > pivots[tid]) && (item <= pivots[tid+1]){
            data.push(item);
            //size_unlocked[tid]= size_unlocked[tid]+1;
            //sizes_local= sizes_local+1;
        }
    }
    
    // TODO: Write local size to shared sizes
    {
        let mut size_unlocked = sizes.lock().unwrap();
        size_unlocked[tid]=data.len() as u64;
        // curly braces to scope our lock guard
    }

    // TODO: Sort local data
    data.sort_by(|a,b| a.partial_cmp(b).unwrap());
    // Here's our printout
    //let mut size_unlocked1 = sizes.lock().unwrap();

    println!("{}: start {}, count {}", tid, &data[0], &data.len());

    //printing local array
    //println!("\nsorted local arrays:\n");
    //println!("{:?}",data);
    bb.wait();

    // TODO: Write data to local buffer
    let mut cur = Cursor::new(vec![]);

    for xx in &data {
        let tmp = xx.to_ne_bytes();
        cur.write_all(&tmp).unwrap();
    }

    //barrier wait
    
    // TODO: Get position for output file
    let prev_count = {
        
        let sze = sizes.lock().unwrap();
        let mut count = 0u64;
        for i in 0..tid{
            count = count + sze[i];
        }

        // curly braces to scope our lock guard
        count
    };
    
    //println!("\nposition... in thr{} is {}", &tid, &prev_count);
    //bb.wait();
    let mut outf = OpenOptions::new()
        .read(true)
        .write(true)
        .open(out_path).unwrap();
    
    // TODO: Seek and write local buffer.
    outf.seek(SeekFrom::Start(8+(4*(prev_count)))).unwrap();
    outf.write_all(cur.get_ref()).unwrap();
        //outf.seek(SeekFrom::Start(4)).unwrap();
    //}



    // TODO: Figure out where the barrier goes.
}
