process VCFPGLOADER {
    tag "$meta.id"
    label 'process_medium'

    conda "${moduleDir}/environment.yml"
    container "${ workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'oras://ghcr.io/zacharyr41/vcf-pg-loader:0.5.4' :
        'ghcr.io/zacharyr41/vcf-pg-loader:0.5.4' }"

    secret 'PGPASSWORD'

    input:
    tuple val(meta), path(vcf), path(tbi)
    val(db_host)
    val(db_port)
    val(db_name)
    val(db_user)
    val(db_schema)

    output:
    tuple val(meta), path("*.load_report.json"), emit: report
    tuple val(meta), path("*.load.log")        , emit: log
    tuple val(meta), env(ROWS_LOADED)          , emit: row_count
    path "versions.yml"                        , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    def batch_size = task.ext.batch_size ?: '10000'
    """
    vcf-pg-loader load \\
        --host ${db_host} \\
        --port ${db_port} \\
        --database ${db_name} \\
        --user ${db_user} \\
        --schema ${db_schema} \\
        --batch ${batch_size} \\
        --sample-id ${meta.id} \\
        --report ${prefix}.load_report.json \\
        --log ${prefix}.load.log \\
        $args \\
        $vcf

    ROWS_LOADED=\$(jq -r '.variants_loaded' ${prefix}.load_report.json)

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        vcfpgloader: \$(vcf-pg-loader --version)
        python: \$(python --version | sed 's/Python //')
        asyncpg: \$(python -c "import asyncpg; print(asyncpg.__version__)")
        cyvcf2: \$(python -c "import cyvcf2; print(cyvcf2.__version__)")
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    echo '{"status": "stub", "variants_loaded": 0, "elapsed_seconds": 0}' > ${prefix}.load_report.json
    touch ${prefix}.load.log
    ROWS_LOADED=0

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        vcfpgloader: \$(vcf-pg-loader --version)
        python: 3.12.0
        asyncpg: 0.29.0
        cyvcf2: 0.31.0
    END_VERSIONS
    """
}
