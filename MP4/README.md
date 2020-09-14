# CS_425_mp4

Maple Juice - CS 425 Fall 19 MP4


## Building the code
`go build main.go maple_juice.go mjHelper.go sdfs.go membership_protocol.go config.go`

## Running the code
`./main`

## Sample on-screen instructions
`./main`

Please give the input. Follwing are the options:

        1. Print membership list
        2. Print my ID
        3. Join the system
        4. Leave the system
        5. Put local file in the filesystem
        6. Get file from filesystem
        7. Delete file from filesystem
        8. ls command - where all given file is present
        9. store command - which files are present in this node
        a. Print fileTable
        b. Print liveNodes
        c. Give maple command
        d. Give juice command

        Press any of the above chars for the required functionality:

## To run maple command
Press c. Enter the command as follows: 

`maple <maple_exe> <num_maples> <sdfs_intermediate_file_prefix> <sdfs_src_directory>`

## To run juice command
Press d. Enter the command as follows: 

`juice <juice_exe> <num_juices> <sdfs_intermediate_file_prefix> <sdfs_dest_filename> delete_input={0,1} partition={0,1}`