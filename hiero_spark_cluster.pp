$hiero_root="$::hiero_run_dir"
$hadoop_folder_name="$::hadoop_folder"
$hdfs_folder="$hiero_root/hdfs-folders"

notify{"hiero_core: $hiero_core":
}
notify{"hiero_folder: $hdfs_folder":
}
notify{"java_home: $java_home":
}

file {'core-site':
      ensure  => file,
      path    => "$hadoop_folder_name/etc/hadoop/core-site.xml",
      mode    => 0755,
      content => template("$hiero_root/puppet-manifests/core-site.xml.erb"),
    }
file {'hdfs-site':
      ensure  => file,
      path    => "$hadoop_folder_name/etc/hadoop/hdfs-site.xml",
      mode    => 0755,
      content => template("$hiero_root/puppet-manifests/hdfs-site.xml.erb"),
    }
file {'hadoop-env':
      ensure  => file,
      path    => "$hadoop_folder_name/etc/hadoop/hadoop-env.sh",
      mode    => 0755,
      content => template("$hiero_root/puppet-manifests/hadoop-env.sh.erb"),
    }
file { "$hdfs_folder":
      ensure => "directory",
      owner  => "lalith",
      group  => "lalith",
      mode   => 755,
  }
