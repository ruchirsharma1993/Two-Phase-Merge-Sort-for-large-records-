
package com;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

/**
 *
 * @author ruchir
 * Running Time: 
 * 5MB, 100MB RAM :- 1 second
 * 50MB 100MB RAM:- 4 second 
 * 500 MB, 100 RAM :- 29 second
 * 1GB , 10000000 40 10 20 38 , 1 minutes 36 seconds
 * 2 GB 100MB RAM: no of entries: 4000000, size of entries: 450, Time: 2 minutes 21 seconds
 * 3 GB, 100 MB 4 minutes 17 seconds

 * 3.7GB, 100 MB  5 minutes 32 seconds
 * 4.1GB, 100MB RAM 10000000 170 150 25 60,  6 minutes 7 seconds
 * 4.4GB, 100MB RAM 10000000 200 150 25 60, 6 minutes 24 seconds
 * 4.9 GB, 100MB RAM 10000000 250 150 25 60, 7 minutes 59 seconds
 * 5.0 GB, 10000000 100 100 100 100 100, 8 minutes 23 seconds

 * 6.2 GB 100MB RAM 9000000 250 250 150 150, 10 minutes 25 seconds
__________________________________________________
* 512 MB 10 MB --
* 512 MB 25 MB RAM 48 seconds
* 512MB 100MB 42 seconds
 * 512MB 250MB 42 seconds
 * 512MB 512MB 45 seconds
 */
 class AscComparator implements Comparator<List<String>>
{
    @Override
    public int compare(List<String> l1,List<String> l2)
    {
        for(int i=0;i<file1.col_order.size();i++)
        {
            if(l1.get(i).equals(l2.get(i))) {
            } 
            else
                return l1.get(i).compareTo(l2.get(i));
        }
        return 0;
    }
};
 class DescComparator implements Comparator<List<String>>
{
    @Override
    public int compare(List<String> l1,List<String> l2)
    {
        for(int i=0;i<file1.col_order.size();i++)
        {
            if(l1.get(i).equals(l2.get(i))) {
            } 
            else
                return (-1*l1.get(i).compareTo(l2.get(i)));
        }
        return 0;
    }
};
public class file1 
{
    
    static List<List<String>> my_data = new ArrayList<>();
    
    public static List<Integer> col_order = new ArrayList();
    public static void main(String args[]) throws Exception
    {
        try{
        //Process and initialize commandline arguments
        //input.txt output.txt 50 asc c0 c1 
         String ipfile = args[0];
         String opfile = args[1];
         String mems = args[2];
         String order = args[3];
             
        int mem_size = Integer.parseInt(mems);
       
        mem_size = (int) (0.1*1000000*mem_size);
        
        //Read Metadata
        List<String> col_list = new ArrayList();
       
       
        
        BufferedReader br = new BufferedReader(new FileReader("/home/ruchir/Downloads/Data Generator/metadata.txt"));
        String read=br.readLine();
        int col_count=0,line_size=0;
        while(read!=null)
        {
            col_count++;
            
            String cname = read.substring(0,read.indexOf(","));
            cname=cname.trim();
           // System.out.println("Added: "+cname);
            col_list.add(cname);
            int csize = Integer.parseInt(read.substring(read.indexOf(",")+1).trim());
            line_size+=csize;
            read=br.readLine();
        }
        
        br.close();
       
        //Read column id's from args and assign order of sorting
        for(int i=4;i<args.length;i++)
        {
            String cur = args[i];
            boolean flag=false;
            for(int j=0;j<col_count;j++)
            {
                if(cur.equals(col_list.get(j)))
                {
                    col_order.add(j);
                    flag=true;
                    break;
                }
            }
            //Thrwo exception if the column required to sort is not in metadata..!
            if(!flag)
                throw new Exception("Error in colname");
        }
        
        //System.out.println("71");
        int lines_toread = (int)mem_size/line_size;
        int count=0;
       int out_no=0;
        //Start Processing and reading the input file
        BufferedReader br1 = new BufferedReader(new FileReader("/home/ruchir/Downloads/Data Generator/input.txt"));
        read=br1.readLine();
        int loc_count=0;
        while(loc_count<lines_toread&&read!=null)
        {
            String cur_line = read;
            StringTokenizer st = new StringTokenizer(cur_line);
            List<String> loc_list = new ArrayList<>();
            while(st.hasMoreElements())
            {
                loc_list.add(st.nextToken());
            }
            my_data.add(loc_list);
            out_no++;
            loc_count++;
            read=br1.readLine();
            if(lines_toread==loc_count||read==null)
            {
                //Sort the all_data
                 if(order.equals("asc"))
                    Collections.sort(my_data,new AscComparator());
                 else
                    Collections.sort(my_data,new DescComparator());
                     
                 write_intofile(count);
                loc_count=0;
                count++;
            }
        }
        if(my_data.size()>0)
        {
             if(order.equals("asc"))
                    Collections.sort(my_data,new AscComparator());
                 else
                    Collections.sort(my_data,new DescComparator());
                 
                write_intofile(count);
                loc_count=0;
                count++;
        }
        System.out.println("No of records read and sorted:"+out_no);
        //Now Merge the files
       /* if(count*line_size>lines_toread)
        {
            throw new Exception("Out of Memory");
        }*/
        int block_size = (int)mem_size/(count*line_size*2);
        int isnull=0;
        BufferedReader br2[] = new BufferedReader[count+2];
        for(int j=0;j<count;j++)
            br2[j] = new BufferedReader(new FileReader("processing"+j));
       // File file = new File("processing"+count);
        boolean[] arr = new boolean[count+2];
        
        File out_file = new File(opfile);
        if (!out_file.exists()) 
        {
           out_file.createNewFile();
        }
        int rec_read=0;
        FileWriter fw = new FileWriter(out_file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);
            
        for(int k=0;k<count;k++)
            arr[k]=true;
        
        //Read all blocks of data into my_data
      
        int last=0;
        //System.out.println(rec_read++);
        //System.out.println("Written first entry");
     //   
        System.out.println("Size: "+my_data.size());
        my_data.clear();

        while(isnull<count)
        {
            if(my_data.size()==0||!arr[last])
            {
                  for(int j=0;j<count;j++)
                  {
                      //System.out.println("in loop"+block_size);
                          if(arr[j])
                          {
                              for(int k=0;k<block_size;k++)
                              {
                                  //System.out.println("Here");
                                  read = br2[j].readLine();
                                  if(read==null)
                                  {
                                      isnull++;
                                      System.out.println("Completed File"+j+" at: "+rec_read);
                                      arr[j]=false;    
                                      break;
                                  }
                                  StringTokenizer st = new StringTokenizer(read);
                                  List<String> loc_list = new ArrayList<>();
                                  while(st.hasMoreElements())
                                  {
                                      loc_list.add(st.nextToken());
                                  }
                                  loc_list.add(""+j);
                                  my_data.add(loc_list);

                                 // System.out.println("Read"+j);
                              }
                          }

                   }

                 if(order.equals("asc"))
                    Collections.sort(my_data,new AscComparator());
                 else
                    Collections.sort(my_data,new DescComparator());
                 
                 // System.out.println("current size of my_data"+my_data.size());
                  List<String> loc1_list = new ArrayList<>(my_data.get(0));
                  my_data.remove(0);
                  for(int k=0;k<loc1_list.size()-1;k++)
                  {
                      bw.write(loc1_list.get(k));
                      if(k!=loc1_list.size()-2)
                         bw.write(" ");
                  }
                  bw.write("\n");
                  last = Integer.parseInt(loc1_list.get(col_count));
                  rec_read++;
            }
             else
            {
                read = br2[last].readLine();
                if(read==null)
                {
                        if(arr[last])
                            isnull++;
                        System.out.println("Completed File"+last+"at "+rec_read);
                        arr[last]=false;    
                        br2[last].close();

                }
                else
                {
                        StringTokenizer st = new StringTokenizer(read);
                        List<String> loc_list = new ArrayList<>();
                        while(st.hasMoreElements())
                        {
                            loc_list.add(st.nextToken());
                        }
                        loc_list.add(""+last);

                        for(int k=0;k<my_data.size();k++)
                        {
                            List<String> list_tocomp = new ArrayList<>(my_data.get(k));
                            if(order.equals("asc"))
                            {
                                if(asc_compare_list(loc_list,list_tocomp)<=0)
                                {
                                    //Write loc_list into output file
                                   // System.out.print(loc_list);
                                    for(int m=0;m<loc_list.size()-1;m++)
                                    {
                                            bw.write(loc_list.get(m));
                                               if(loc_list.size()-2!=m)
                                                   bw.write("  ");
                                    }
                                    bw.write("\n");
                                    rec_read++;
                                    loc_list.clear();
                                    break;
                                }
                                else
                                {
                                    //Write my_data.get(k) into output file
                                    //System.out.print(my_data.get(k));
                                     for(int m=0;m<list_tocomp.size()-1;m++)
                                    {
                                            bw.write(list_tocomp.get(m));
                                               if(list_tocomp.size()-2!=m)
                                                   bw.write(" ");
                                    }
                                    bw.write("\n");
                                   //  System.out.println(last+":"+rec_read++);
                                    rec_read++;
                                    last=Integer.parseInt(list_tocomp.get(col_count));
                                    my_data.remove(k);
                                    //list_tocomp.clear();

                                }
                            }
                            else
                            {
                                if(desc_compare_list(loc_list,list_tocomp)<=0)
                                {
                                    //Write loc_list into output file
                                   // System.out.print(loc_list);
                                    for(int m=0;m<loc_list.size()-1;m++)
                                    {
                                            bw.write(loc_list.get(m));
                                               if(loc_list.size()-2!=m)
                                                   bw.write(" ");
                                    }
                                    bw.write("\n");
                                    rec_read++;
                                    loc_list.clear();
                                    break;
                                }
                                else
                                {
                                    //Write my_data.get(k) into output file
                                    //System.out.print(my_data.get(k));
                                     for(int m=0;m<list_tocomp.size()-1;m++)
                                    {
                                            bw.write(list_tocomp.get(m));
                                               if(list_tocomp.size()-2!=m)
                                                   bw.write(" ");
                                    }
                                    bw.write("\n");
                                   //  System.out.println(last+":"+rec_read++);
                                    rec_read++;
                                    last=Integer.parseInt(list_tocomp.get(col_count));
                                    my_data.remove(k);
                                    //list_tocomp.clear();

                                }

                            }
                        }


                     
                }
            }
           
        
        }
        
      if(my_data.size()>0)
      {
          //Write remaining data into text file
          System.out.println("Remaining Size" +my_data.size());
           for(int k=0;k<my_data.size();k++)
           {
                          
                 //Write my_data.get(k) into output file
             List<String> loc_out = new ArrayList<>(my_data.get(k));
                for(int m=0;m<loc_out.size()-1;m++)
                {
                        bw.write(loc_out.get(m));
                           if(loc_out.size()-2!=m)
                              bw.write(" ");
                }
                bw.write("\n");
               // System.out.println(last+":"+rec_read++);
                 last=Integer.parseInt(loc_out.get(col_count));
                 
                 rec_read++;

                            
             }
      }
        bw.close(); //Close output file
        System.out.println(last+":"+rec_read);
        }
        catch(Exception e)
        {
            System.out.println("Exception: "+e);
            
        }
    }

    private static void write_intofile(int count) throws Exception
    {
       try{ File file = new File("processing"+count);
 
	// if file doesnt exists, then create it
	if (!file.exists()) 
        {
            file.createNewFile();
	}
 
	FileWriter fw = new FileWriter(file.getAbsoluteFile());
	BufferedWriter bw = new BufferedWriter(fw);
	for(int i=0;i<my_data.size();i++)
        {
            List<String> loc_list = new ArrayList<>(my_data.get(i));
            for(int j=0;j<loc_list.size();j++)
            {
                bw.write(loc_list.get(j));
                if(j!=loc_list.size()-1)
                    bw.write("  ");
            }
            bw.write("\n");
        }
        bw.close();
        my_data.clear();
    }catch(Exception e){System.out.println("Exception in writing file"+e.getLocalizedMessage());}
    }
    private static int asc_compare_list(List<String> l1, List<String> l2) 
    {
        for(int i=0;i<file1.col_order.size();i++)
        {
            if(l1.get(i).equals(l2.get(i))) {
            } 
            else
                return l1.get(i).compareTo(l2.get(i));
        }
        return 0;
     }
    private static int desc_compare_list(List<String> l1, List<String> l2) 
    {
        for(int i=0;i<file1.col_order.size();i++)
        {
            if(l1.get(i).equals(l2.get(i))) {
            } 
            else
                return (-1*l1.get(i).compareTo(l2.get(i)));
        }
        return 0;
     }
    
}
