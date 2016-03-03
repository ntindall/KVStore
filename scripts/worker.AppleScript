on run {pwd, n, i} -- each arg is a string
    tell application "Terminal"
      set shell to do script "stack exec kvstore-slave -- -l -n " & n & " -i " & i 
      do script "cd " & pwd in shell
  end tell
end run