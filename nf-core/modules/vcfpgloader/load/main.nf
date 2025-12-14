process VCFPGLOADER_LOAD {
    tag "$meta.id"
    label 'process_low'

    conda "${moduleDir}/environment.yml"
    container "ghcr.io/zacharyr41/vcf-pg-loader:0.4.0"

    input:
    tuple val(meta), path(vcf)

    output:
    tuple val(meta), path("*.load_report.json"), emit: report
    path "versions.yml"                        , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    def args = task.ext.args ?: ''
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    vcf-pg-loader load \\
        $args \\
        --report ${prefix}.load_report.json \\
        $vcf

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        vcfpgloader: \$(vcf-pg-loader --version)
    END_VERSIONS
    """

    stub:
    def prefix = task.ext.prefix ?: "${meta.id}"
    """
    echo '{"status":"stub","variants_loaded":0,"load_batch_id":"00000000-0000-0000-0000-000000000000"}' > ${prefix}.load_report.json

    cat <<-END_VERSIONS > versions.yml
    "${task.process}":
        vcfpgloader: \$(vcf-pg-loader --version)
    END_VERSIONS
    """
}
