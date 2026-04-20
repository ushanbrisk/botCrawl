#!/bin/bash
#=============================================================================
# 带进度记录的SCP脚本
# 用途：从远程机器批量scp文件到本地，支持断点续传
# 
# 使用方法：
#   1. 首次执行：直接运行脚本
#   2. 继续执行：直接运行脚本（会自动跳过已成功的文件）
#   3. 查看进度：cat progress.txt
#   4. 重置进度：rm progress.txt
#=============================================================================

# 配置参数
REMOTE_DIR="/ssd/music/song_download"
LOCAL_DIR="/ssd12/music/song_download"
LOCAL_USER="luke"
LOCAL_HOST="big"
SSH_KEY=""  # 如果需要指定密钥，取消注释并填入路径，如：SSH_KEY="-i ~/.ssh/id_rsa"

# 进度记录文件
PROGRESS_FILE="scp_progress.txt"
LOG_FILE="scp_error.log"

#=============================================================================
# 函数定义
#=============================================================================

# 初始化：读取已完成的文件列表
load_progress() {
    if [[ -f "$PROGRESS_FILE" ]]; then
        while IFS= read -r line; do
            # 跳过空行和注释
            [[ -z "$line" || "$line" =~ ^# ]] && continue
            COMPLETED_FILES["$line"]=1
        done < "$PROGRESS_FILE"
        echo "已加载 $((${#COMPLETED_FILES[@]})) 个已完成的任务"
    else
        echo "未找到进度文件，将从头开始"
    fi
}

# 记录进度
save_progress() {
    local file="$1"
    echo "$file" >> "$PROGRESS_FILE"
    COMPLETED_FILES["$file"]=1
}

# 检查文件是否已完成
is_completed() {
    local file="$1"
    [[ -n "${COMPLETED_FILES[$file]}" ]]
}

# 获取所有待传输的文件列表
get_file_list() {
    # 获取所有.mp3文件（根据实际需求修改扩展名）
    ls "$REMOTE_DIR"/*.mp3 2>/dev/null
}

# 单个文件传输
scp_file() {
    local file="$1"
    local filename=$(basename "$file")
    
    echo -n "传输 $filename ... "
    
    # 执行scp命令
    scp $SSH_KEY "$file" "${LOCAL_USER}@${LOCAL_HOST}:${LOCAL_DIR}/"
    
    if [[ $? -eq 0 ]]; then
        echo "成功"
        save_progress "$filename"
        return 0
    else
        echo "失败"
        echo "$filename" >> "$LOG_FILE"
        return 1
    fi
}

# 主函数
main() {
    local total=0
    local success=0
    local skipped=0
    local failed=0
    
    echo "============================================"
    echo "SCP批量传输脚本 - 带进度记录"
    echo "============================================"
    echo "远程目录: $REMOTE_DIR"
    echo "本地目录: ${LOCAL_USER}@${LOCAL_HOST}:${LOCAL_DIR}"
    echo "进度文件: $PROGRESS_FILE"
    echo "============================================"
    
    # 初始化进度
    declare -A COMPLETED_FILES
    load_progress
    
    # 获取文件列表
    echo ""
    echo "扫描文件..."
    files=$(get_file_list)
    
    if [[ -z "$files" ]]; then
        echo "没有找到需要传输的文件"
        exit 0
    fi
    
    # 统计文件数量
    file_count=$(echo "$files" | wc -l)
    echo "共找到 $file_count 个文件"
    echo ""
    
    # 开始传输
    echo "开始传输..."
    echo "--------------------------------------------"
    
    for file in $files; do
        local filename=$(basename "$file")
        ((total++))
        
        if is_completed "$filename"; then
            echo "[$total/$file_count] 跳过 $filename (已完成)"
            ((skipped++))
        else
            if scp_file "$file"; then
                ((success++))
            else
                ((failed++))
            fi
        fi
        
        # 每传输10个文件刷新一下进度
        if (( total % 10 == 0 )); then
            echo "--- 进度: $total/$file_count, 成功: $success, 跳过: $skipped, 失败: $failed ---"
        fi
    done
    
    # 输出统计
    echo "============================================"
    echo "传输完成！"
    echo "总计: $total"
    echo "成功: $success"
    echo "跳过: $skipped"
    echo "失败: $failed"
    echo "============================================"
    
    if [[ $failed -gt 0 ]]; then
        echo "失败的文件已记录在: $LOG_FILE"
        echo "请检查网络连接后重新运行脚本"
    fi
}

#=============================================================================
# 脚本入口
#=============================================================================
main "$@"
